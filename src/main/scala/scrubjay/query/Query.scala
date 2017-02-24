package scrubjay.query

import javax.management.relation.RelationType

import scrubjay._
import scrubjay.metabase._
import scrubjay.datasource._
import gov.llnl.ConstraintSolver._
import scrubjay.derivation.NaturalJoin
import scrubjay.metabase.MetaDescriptor.DimensionSpace.DimensionSpace
import scrubjay.metabase.MetaDescriptor.{DimensionSpace, MetaDimension, MetaRelationType}
import scrubjay.metasource.MetaSource
import scrubjay.units.UnitsTag.DomainType
import scrubjay.units.UnitsTag.DomainType.DomainType


class Query(val dataSources: Set[ScrubJayRDD],
            val metaEntries: Set[MetaEntry]) {

  def run: Iterator[ScrubJayRDD] = {

    // Can we join ds1 and ds2?
    lazy val joinedPair: Constraint[ScrubJayRDD] = memoize(args => {
      val ds1 = args(0).as[ScrubJayRDD]
      val ds2 = args(1).as[ScrubJayRDD]

      // 1. Do they share a domain dimension?
      val commonDimensions: Seq[(MetaDimension, MetaEntry, MetaEntry)] = {
        MetaSource.commonDimensionEntries(ds1.metaSource, ds2.metaSource)
          .filter(e => Seq(e._2.relationType, e._3.relationType).forall(_ == MetaRelationType.DOMAIN))
      }

      // 2. What are the units of the shared dimensions?
      implicit class sharedProperties(meTuple: (MetaEntry, MetaEntry)) {
        def dimensionTypes(dimType1: DimensionSpace, dimType2: DimensionSpace): Boolean = {
          meTuple._1.dimension.dimensionType == dimType1 &&
          meTuple._2.dimension.dimensionType == dimType2
        }
        def domainTypes(domType1: DomainType, domType2: DomainType): Boolean = {
          meTuple._1.units.unitsTag.domainType == domType1 &&
            meTuple._2.units.unitsTag.domainType == domType2
        }
      }

      commonDimensions.flatMap {

        // Discrete, Discrete
        case (_, me1, me2) if (me1, me2).dimensionTypes(DimensionSpace.DISCRETE, DimensionSpace.DISCRETE) => {

          (me1, me2) match {

            // Discrete Point, Discrete Point
            case me12 if me12.domainTypes(DomainType.POINT, DomainType.POINT) =>
              ds1.deriveNaturalJoin(Some(ds2))

            // Discrete Point, Discrete Range
            case me12 if me12.domainTypes(DomainType.POINT, DomainType.RANGE) =>
              ds1.deriveNaturalJoin(ds2.deriveExplodeList(ds2.metaSource.columnForEntry(me2).toSeq))

            // Discrete Range, Discrete Point
            case me12 if me12.domainTypes(DomainType.RANGE, DomainType.POINT) =>
              ds2.deriveNaturalJoin(ds1.deriveExplodeList(ds1.metaSource.columnForEntry(me1).toSeq))

            case _ => None
          }
        }

        // Continuous, Continuous
        case (_, me1, me2) if (me1, me2).dimensionTypes(DimensionSpace.CONTINUOUS, DimensionSpace.CONTINUOUS) => {

          // TODO: Get window size from meta information
          (me1, me2) match {

            // Continuous Point, Continuous Point
            case me12 if me12.domainTypes(DomainType.POINT, DomainType.POINT) =>
              ds1.deriveInterpolationJoin(Some(ds2), 1000)

            // Continuous Point, Continuous Range
            case me12 if me12.domainTypes(DomainType.POINT, DomainType.RANGE) =>
              ds1.deriveInterpolationJoin(ds2.deriveExplodeTimeSpan(Seq((ds2.metaSource.columnForEntry(me2).get, 1000))), 1000)

            // Continuous Range, Continuous Point
            case me12 if me12.domainTypes(DomainType.RANGE, DomainType.POINT) =>
              ds2.deriveInterpolationJoin(ds1.deriveExplodeTimeSpan(Seq((ds1.metaSource.columnForEntry(me1).get, 1000))), 1000)

            case _ => None
          }
        }
      }
    })

    // Can we join a set of datasources dsSet?
    lazy val joinedSet: Constraint[ScrubJayRDD] = memoize(args => {
      val dsSet: Set[ScrubJayRDD] = args(0).as[Set[ScrubJayRDD]]

      dsSet.toSeq match {

        // One element
        case Seq(ds1) => Seq(ds1)

        // Two elements, check joined pair
        case Seq(ds1, ds2) => joinedPair(Seq(ds1, ds2))

        // More than two elements...
        case head +: tail =>

          // joinPair( head, joinSet(tail) )
          val restThenPair = joinedSet(Seq(tail.toSet[ScrubJayRDD])).flatMap(tailSolution => joinedPair(Seq(head, tailSolution)))

          // Set of all joinable pairs between head and some t in tail
          val head2TailPairs = new ArgumentSpace(Seq(head), tail).allSolutions(joinedPair)

          // joinSet( joinPair(head, t) +: rest )
          val pairThenRest = head2TailPairs.flatMap(pair => {
            val pairArgs = pair.arguments.map(_.as[ScrubJayRDD])
            val rest = dsSet.filterNot(pairArgs.contains)
            pair.solutions.flatMap(sol =>  {
              joinedSet(Seq(rest + sol))
            })
          })

          restThenPair ++ pairThenRest
      }
    })

    // Can I derive a datasource from the set of datasources that satisfies my query?
    lazy val dsSetSatisfiesQuery: Constraint[ScrubJayRDD] = memoize(args => {
      val query = args(0).as[Set[MetaEntry]]
      val dsSet = args(1).as[Set[ScrubJayRDD]]

      // Fun case: queried meta entries exist in a data source derived from multiple data sources
      val dsMeta = dsSet.toSeq.map(_.metaSource.metaEntryMap.values.toSet).reduce(_ union _)
      val metaSatisfied = query.intersect(dsMeta).size == query.size

      if (metaSatisfied)
        joinedSet(Seq(dsSet))
      else
        Seq.empty
    })

    // TODO: optimize the order of choosing
    // 1. only datasources that satisfy part of the query
    // 2. add in additional other datasources one at a time
    class ChooseNDataSources(me: Seq[MetaEntry], ds: Seq[ScrubJayRDD]) extends ArgumentSpace {
      override def enumerate: Iterator[Arguments] = {
        (1 until ds.length+1).toIterator.flatMap(
          ds.combinations(_).map(c => Seq(metaEntries, c.toSet[ScrubJayRDD]))
        )
      }
    }

    val argSpace = new ChooseNDataSources(metaEntries.toSeq, dataSources.toSeq)

    argSpace.allSolutions(dsSetSatisfiesQuery).flatMap(_.solutions)
  }
}
