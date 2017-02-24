package scrubjay.query

import javax.management.relation.RelationType

import scrubjay._
import scrubjay.metabase._
import scrubjay.datasource._
import gov.llnl.ConstraintSolver._
import scrubjay.derivation.NaturalJoin
import scrubjay.metabase.MetaDescriptor.{DimensionSpace, MetaDimension, MetaRelationType}
import scrubjay.metasource.MetaSource
import scrubjay.units.UnitsTag.DomainType


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
      commonDimensions.flatMap{

        // Discrete, Discrete => Natural Join
        case (_,
              MetaEntry(_, _, MetaDimension(_, _, DimensionSpace.DISCRETE), _),
              MetaEntry(_, _, MetaDimension(_, _, DimensionSpace.DISCRETE), _)) => {
          ds1.deriveNaturalJoin(Some(ds2))
        }

        // Point, Point => Interpolation Join
        case (_,
              MetaEntry(_, _, MetaDimension(_, _, DimensionSpace.CONTINUOUS), units1),
              MetaEntry(_, _, MetaDimension(_, _, DimensionSpace.CONTINUOUS), units2))
        if Seq(units1, units2).forall(_.unitsTag.domainType == DomainType.POINT) => {
          ds1.deriveInterpolationJoin(Some(ds2), 1000 /* WINDOW SIZE ??? */ )
        }

        // Point, Range => explode range, interpolation join
        case (_,
              MetaEntry(_, _, MetaDimension(_, _, DimensionSpace.CONTINUOUS), units1),
              me2 @ MetaEntry(_, _, MetaDimension(_, _, DimensionSpace.CONTINUOUS), units2))
        if units1.unitsTag.domainType == DomainType.POINT && units2.unitsTag.domainType == DomainType.RANGE => {
          ds1.deriveInterpolationJoin(ds2.deriveExplodeList(Seq(ds2.metaSource.columnForEntry(me2)).flatten), 1000 /* WINDOW SIZE ??? */ )
          None
        }

        // Range, Point => explode range, interpolation join
        case (_,
          me1 @ MetaEntry(_, _, MetaDimension(_, _, DimensionSpace.CONTINUOUS), units1),
          MetaEntry(_, _, MetaDimension(_, _, DimensionSpace.CONTINUOUS), units2))
        if units1.unitsTag.domainType == DomainType.POINT && units2.unitsTag.domainType == DomainType.RANGE => {
          ds2.deriveInterpolationJoin(ds1.deriveExplodeList(Seq(ds1.metaSource.columnForEntry(me1)).flatten), 1000 /* WINDOW SIZE ??? */ )
          None
        }

        // Can't join
        case _ => None
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
