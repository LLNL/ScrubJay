package scrubjay.query

import scrubjay.metabase._
import scrubjay.datasource._

import gov.llnl.ConstraintSolver._


case class Query(dataSources: Set[DataSourceID],
                 metaEntries: Set[MetaEntry]) {

  // Can we join dsID1 and dsID2?
  lazy val joinedPair: Constraint[DataSourceID] = memoize(args => {
    val dsID1 = args(0).as[DataSourceID]
    val dsID2 = args(1).as[DataSourceID]

    JoinSpace(dsID1, dsID2).enumerate.toSeq
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

  def solutions: Iterator[DataSourceID] = {
    QuerySpace(metaEntries, dataSources.toSeq)
      .allSolutions(dsIDSetSatisfiesQuery).flatMap(_.solutions)
  }

  def allDerivations: Iterator[DataSourceID] = {
    DerivationSpace(dataSources.toSeq)
      .allSolutions(joinedSet).flatMap(_.solutions)
  }
}
