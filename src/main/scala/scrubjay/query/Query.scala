package scrubjay.query

import javax.management.relation.RelationType

import scrubjay._
import scrubjay.metabase._
import scrubjay.metasource._
import scrubjay.metasource.MetaSourceImplicits
import scrubjay.datasource._
import gov.llnl.ConstraintSolver._
import scrubjay.derivation._
import scrubjay.util._
import scrubjay.metabase.MetaDescriptor.{DimensionSpace, MetaDimension, MetaRelationType}
import scrubjay.metasource.MetaSource
import scrubjay.units.UnitsTag.DomainType


class Query(val dataSources: Set[DataSourceID],
            val metaEntries: Set[MetaEntry]) {

  def run: Iterator[DataSourceID] = {

    // Can we join dsID1 and dsID2?
    lazy val joinedPair: Constraint[DataSourceID] = memoize(args => {
      val dsID1 = args(0).as[DataSourceID]
      val dsID2 = args(1).as[DataSourceID]

      // 1. Do they share a domain dimension?
      val commonDimensions: Seq[(MetaDimension, MetaEntry, MetaEntry)] = {
        MetaSource.commonDimensionEntries(dsID1.metaSource, dsID2.metaSource)
          .filter(e => Seq(e._2.relationType, e._3.relationType).forall(_ == MetaRelationType.DOMAIN))
      }

      // 2. What are the units of the shared dimensions?
      commonDimensions.flatMap{

        // Discrete, Discrete => Natural Join
        case (_,
              MetaEntry(_, _, MetaDimension(_, _, DimensionSpace.DISCRETE), _),
              MetaEntry(_, _, MetaDimension(_, _, DimensionSpace.DISCRETE), _)) => {
          //DataSourceID("natural join")(Seq(dsID1.ID, dsID2.ID))
          NaturalJoin(dsID1, dsID2)
        }

        // Point, Point => Interpolation Join
        case (_,
              MetaEntry(_, _, MetaDimension(_, _, DimensionSpace.CONTINUOUS), units1),
              MetaEntry(_, _, MetaDimension(_, _, DimensionSpace.CONTINUOUS), units2))
        if Seq(units1, units2).forall(_.unitsTag.domainType == DomainType.POINT) => {
          InterpolationJoin(dsID1, dsID2, 1000 /* WINDOW SIZE ??? */ )
        }

        // Point, Range => explode range, interpolation join
        case (_,
              MetaEntry(_, _, MetaDimension(_, _, DimensionSpace.CONTINUOUS), units1),
              me2 @ MetaEntry(_, _, MetaDimension(_, _, DimensionSpace.CONTINUOUS), units2))
        if units1.unitsTag.domainType == DomainType.POINT && units2.unitsTag.domainType == DomainType.RANGE => {
          InterpolationJoin(dsID1, ExplodeList(dsID2, Seq(dsID2.metaSource.columnForEntry(me2)).flatten).get, 1000 /* WINDOW SIZE ??? */ )
        }

        // Range, Point => explode range, interpolation join
        case (_,
          me1 @ MetaEntry(_, _, MetaDimension(_, _, DimensionSpace.CONTINUOUS), units1),
          MetaEntry(_, _, MetaDimension(_, _, DimensionSpace.CONTINUOUS), units2))
        if units1.unitsTag.domainType == DomainType.POINT && units2.unitsTag.domainType == DomainType.RANGE => {
          InterpolationJoin(dsID2, ExplodeList(dsID1, Seq(dsID1.metaSource.columnForEntry(me1)).flatten).get, 1000 /* WINDOW SIZE ??? */ )
        }

        // Can't join
        case _ => None
      }
    })

    // Can we join a set of datasources dsIDSet?
    lazy val joinedSet: Constraint[DataSourceID] = memoize(args => {
      val dsIDSet: Set[DataSourceID] = args(0).as[Set[DataSourceID]]

      dsIDSet.toSeq match {

        // One element
        case Seq(dsID1) => Seq(dsID1)

        // Two elements, check joined pair
        case Seq(dsID1, dsID2) => joinedPair(Seq(dsID1, dsID2))

        // More than two elements...
        case head +: tail =>

          // joinPair( head, joinSet(tail) )
          val restThenPair = joinedSet(Seq(tail.toSet[DataSourceID])).flatMap(tailSolution => joinedPair(Seq(head, tailSolution)))

          // Set of all joinable pairs between head and some t in tail
          val head2TailPairs = new ArgumentSpace(Seq(head), tail).allSolutions(joinedPair)

          // joinSet( joinPair(head, t) +: rest )
          val pairThenRest = head2TailPairs.flatMap(pair => {
            val pairArgs = pair.arguments.map(_.as[DataSourceID])
            val rest = dsIDSet.filterNot(pairArgs.contains)
            pair.solutions.flatMap(sol =>  {
              joinedSet(Seq(rest + sol))
            })
          })

          restThenPair ++ pairThenRest
      }
    })

    // Can I derive a datasource from the set of datasources that satisfies my query?
    lazy val dsIDSetSatisfiesQuery: Constraint[DataSourceID] = memoize(args => {
      val query = args(0).as[Set[MetaEntry]]
      val dsIDSet = args(1).as[Set[DataSourceID]]

      // Fun case: queried meta entries exist in a data source derived from multiple data sources
      val dsIDMeta = dsIDSet.toSeq.map(_.metaSource.values.toSet).reduce(_ union _)
      val metaSatisfied = query.intersect(dsIDMeta).size == query.size

      if (metaSatisfied)
        joinedSet(Seq(dsIDSet))
      else
        Seq.empty
    })

    // TODO: optimize the order of choosing
    // 1. only datasources that satisfy part of the query
    // 2. add in additional other datasources one at a time
    class ChooseNDataSources(me: Seq[MetaEntry], dsID: Seq[DataSourceID]) extends ArgumentSpace {
      override def enumerate: Iterator[Arguments] = {
        (1 until dsID.length+1).toIterator.flatMap(
          dsID.combinations(_).map(c => Seq(metaEntries, c.toSet[DataSourceID]))
        )
      }
    }

    val argSpace = new ChooseNDataSources(metaEntries.toSeq, dataSources.toSeq)

    argSpace.allSolutions(dsIDSetSatisfiesQuery).flatMap(_.solutions)
  }
}
