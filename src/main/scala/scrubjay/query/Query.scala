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
      val me = args(0).as[Set[MetaEntry]]
      val ds = args(1).as[DataSource]

      ds match {
        case dsSat if dsSat.containsMeta(me) => Seq(dsSat)
        case _ => Seq.empty
      }
    })

    // Can we join ds1 and ds2?
    lazy val joinedPair: Constraint[DataSource] = memoize(args => {
      val ds1 = args(0).as[DataSource]
      val ds2 = args(1).as[DataSource]

      // TODO: return all possible joins (a,b), (b,a), natural, quanti, etc
      Seq(
        deriveNaturalJoin(ds1, ds2, sjs)
      ).flatten
    })

    // Can we join a set of datasources dsSet?
    lazy val joinedSet: Constraint[DataSource] = memoize(args => {
      val dsSet: Set[DataSource] = args(0).as[Set[DataSource]]

      dsSet.toSeq match {

        // One element
        case Seq(ds1) => Seq(ds1)

        // Two elements, check joined pair
        case Seq(ds1, ds2) => joinedPair(Seq(Arg(ds1), Arg(ds2)))

        // More than two elements...
        case head +: tail => {


          // joinPair( head, joinSet(tail) )
          val restThenPair = joinedSet(Seq(Arg(tail.toSet))).flatMap(tailSolution => joinedPair(Seq(Arg(head), Arg(tailSolution))))

          // Set of all joinable pairs between head and some t in tail
          val head2TailPairs = new ArgumentSpace(Seq(Arg(head)), tail.map(Arg(_))).allSolutions(joinedPair)

          // joinSet( joinPair(head, t) +: rest )
          val pairThenRest = head2TailPairs.flatMap(pair => {
            val pairArgs = pair.arguments.map(_.as[DataSource])
            val rest = dsSet.filterNot(pairArgs.contains)
            pair.solutions.flatMap(sol =>  {
              joinedSet(Seq(Arg(rest + sol)))
            })
          })

          restThenPair ++ pairThenRest
        }
      }
    })

    // Can I derive a datasource from the set of datasources that satisfies my query?
    lazy val dsSetSatisfiesQuery: Constraint[DataSource] = memoize(args => {
      val query = args(0).as[Set[MetaEntry]]
      val dsSet = args(1).as[Set[DataSource]]

      // Fun case: queried meta entries exist in a data source derived from multiple data sources
      // TODO: return all unique sets of datasources that satisfy the query
      val dsMeta = dsSet.toSeq.map(_.metaSource.metaEntryMap.values.toSet).reduce(_ union _)
      val metaSatisfied = query.intersect(dsMeta).size == query.size

      if (metaSatisfied)
        joinedSet(Seq(Arg(dsSet)))
      else
        Seq.empty
    })

    class ChooseNDataSources(me: Seq[MetaEntry], ds: Seq[DataSource]) extends ArgumentSpace {
      override def enumerate: Iterator[Arguments] = {
        (1 until ds.length+1).toIterator.flatMap(
          ds.combinations(_).map(c => Seq(Arg(metaEntries), Arg(c.toSet)))
        )
      }
    }

    val argSpace = new ChooseNDataSources(metaEntries.toSeq, dataSources.toSeq)

    argSpace.allSolutions(dsSetSatisfiesQuery).flatMap(_.solutions)
  }

}

object Query {
  implicit class ScrubJaySession_Query(sjs: ScrubJaySession) {
    def runQuery(dataSources: Set[DataSource], metaEntries: Set[MetaEntry]): Iterator[DataSource] = {
      new Query(sjs, dataSources, metaEntries).run
    }
  }
}
