package scrubjay.query

import scrubjay._
import scrubjay.meta._
import scrubjay.datasource._

import scrubjay.derivation.NaturalJoin._

import ConstraintSolver._


class Query(val sjs: ScrubJaySession,
            val dataSources: Set[DataSource],
            val metaEntries: Set[MetaEntry]) {

  def run: Iterator[DataSource] = {

    // Easy case: queried meta entries all contained in a single data source
    lazy val dsWithMetaEntries: Constraint[DataSource] = memoize(args => {
      val me: Set[MetaEntry] = args.getAs[Set[MetaEntry]](0)
      val ds: DataSource = args.getAs[DataSource](1)

      ds match {
        case dsSat if dsSat.containsMeta(me) => Some(dsSat)
        case _ => None
      }
    })

    // Can we join ds1 and ds2?
    lazy val joinedPair: Constraint[DataSource] = memoize(args => {
      val ds1 = args.getAs[DataSource](0)
      val ds2 = args.getAs[DataSource](1)

      val joined: Option[DataSource] = deriveNaturalJoin(ds1, ds2, sjs)

      if (joined.isDefined)
        joined
      else
        None
    })

    // Can we join a set of datasources dsSet?
    lazy val joinedSet: Constraint[DataSource] = memoize(args => {
      val dsSet: Set[DataSource] = args.getAs[Set[DataSource]](0)

      dsSet.toSeq match {

        // One element
        case Seq(ds1) => Some(ds1)

        // Two elements, check joined pair
        case Seq(ds1, ds2) => joinedPair(Arguments(ds1, ds2))

        // More than two elements...
        case head +: tail => {

          // Check if head can join with any of tail
          val joinedOptionOption = tail.map(t => joinedPair(Arguments(head, t))).find(_.isDefined)

          // If so, check if the tail is a joinableSet
          if (joinedOptionOption.isDefined) {

            val joined = joinedOptionOption.get.get
            val joinedTail = joinedSet(Arguments(tail.toSet))

            // If so, return the head joined with the joinedSet of the tail
            if (joinedTail.isDefined) {
              joinedPair(Arguments(joined, joinedTail))
            }
            else {
              None
            }

          }
          else {
            None
          }
        }
      }
    })

    // Can I derive a datasource from the set of datasources that satisfies my query?
    lazy val dsSetSatisfiesQuery: Constraint[DataSource] = memoize(args => {
      val query = args.getAs[Set[MetaEntry]](0)
      val dsSet = args.getAs[Set[DataSource]](1)

      // Fun case: queried meta entries exist in a data source derived from multiple data sources
      val dsMeta = dsSet.toSeq.map(_.metaSource.metaEntryMap.values.toSet).reduce(_ union _)
      val metaSatisfied = query.intersect(dsMeta).size == query.size

      if (metaSatisfied)
        joinedSet(Arguments(dsSet))
      else
        None
    })

    class ChooseNDataSources(me: Seq[MetaEntry], ds: Seq[DataSource]) extends ArgumentSpace {
      override def enumerate: Iterator[Arguments] = {
        (1 until ds.length+1).toIterator.flatMap(
          ds.combinations(_).map(c => Arguments(metaEntries, c.toSet))
        )
      }
    }

    val argSpace = new ChooseNDataSources(metaEntries.toSeq, dataSources.toSeq)

    argSpace.allSolutions(dsSetSatisfiesQuery).flatMap(dsSetSatisfiesQuery(_))
  }

}

object Query {
  implicit class ScrubJaySession_Query(sjs: ScrubJaySession) {
    def runQuery(dataSources: Set[DataSource], metaEntries: Set[MetaEntry]): Iterator[DataSource] = {
      new Query(sjs, dataSources, metaEntries).run
    }
  }
}
