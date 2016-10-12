package scrubjay.query

import scrubjay._
import scrubjay.meta._
import scrubjay.datasource._
import gov.llnl.ConstraintSolver._
import scrubjay.derivation.NaturalJoin


class Query(val dataSources: Set[DataSource],
            val metaEntries: Set[MetaEntry]) {

  def run: Iterator[DataSource] = {

    // Can we join ds1 and ds2?
    lazy val joinedPair: Constraint[DataSource] = memoize(args => {
      val ds1 = args(0).as[DataSource]
      val ds2 = args(1).as[DataSource]

      // TODO: return all possible joins (a,b), (b,a), natural, quanti, etc
      Seq(
        new NaturalJoin(ds1, ds2).apply
      ).flatten
    })

    // Can we join a set of datasources dsSet?
    lazy val joinedSet: Constraint[DataSource] = memoize(args => {
      val dsSet: Set[DataSource] = args(0).as[Set[DataSource]]

      dsSet.toSeq match {

        // One element
        case Seq(ds1) => Seq(ds1)

        // Two elements, check joined pair
        case Seq(ds1, ds2) => joinedPair(Seq(ds1, ds2))

        // More than two elements...
        case head +: tail => {

          // joinPair( head, joinSet(tail) )
          val restThenPair = joinedSet(Seq(tail.toSet[DataSource])).flatMap(tailSolution => joinedPair(Seq(head, tailSolution)))

          // Set of all joinable pairs between head and some t in tail
          val head2TailPairs = new ArgumentSpace(Seq(head), tail).allSolutions(joinedPair)

          // joinSet( joinPair(head, t) +: rest )
          val pairThenRest = head2TailPairs.flatMap(pair => {
            val pairArgs = pair.arguments.map(_.as[DataSource])
            val rest = dsSet.filterNot(pairArgs.contains)
            pair.solutions.flatMap(sol =>  {
              joinedSet(Seq(rest + sol))
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
    class ChooseNDataSources(me: Seq[MetaEntry], ds: Seq[DataSource]) extends ArgumentSpace {
      override def enumerate: Iterator[Arguments] = {
        (1 until ds.length+1).toIterator.flatMap(
          ds.combinations(_).map(c => Seq(metaEntries, c.toSet[DataSource]))
        )
      }
    }

    val argSpace = new ChooseNDataSources(metaEntries.toSeq, dataSources.toSeq)

    argSpace.allSolutions(dsSetSatisfiesQuery).flatMap(_.solutions)
  }
}
