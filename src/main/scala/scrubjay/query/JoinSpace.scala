package scrubjay.query

import scrubjay.datasource._
import scrubjay.combination.UberJoin

import gov.llnl.ConstraintSolver._

object JoinSpace {

  // Figure out what kind of join to do, then do it
  lazy val joinedPair: Constraint[DataSourceID] = memoize(args => {
    val dsID1 = args(0).as[DataSourceID]
    val dsID2 = args(1).as[DataSourceID]

    UberJoin(dsID1, dsID2)
  })


  // Can we join a set of datasources dsIDSet?
  lazy val joinedSet: Constraint[DataSourceID] = memoize(args => {
    val dsIDSet: Set[DataSourceID] = args(0).as[Set[DataSourceID]]

    dsIDSet.toSeq match {

      // No elements
      case Seq() => Seq()

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

}
