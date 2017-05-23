package scrubjay.query

import scrubjay.datasetid._
import scrubjay.datasetid.combination.NaturalJoin
import scrubjay.dataspace.{DataSpace, DimensionSpace}
//import scrubjay.datasetid.combination.UberJoin

import gov.llnl.ConstraintSolver._

object JoinSpace {

  // Figure out what kind of join to do, then do it
  lazy val joinedPair: Constraint[DatasetID] = memoize(args => {
    val dimensionSpace = args(0).as[DimensionSpace]
    val dsID1 = args(1).as[DatasetID]
    val dsID2 = args(2).as[DatasetID]

    Seq(NaturalJoin(dsID1, dsID2).asOption(dimensionSpace)).flatten
    //UberJoin(dsID1, dsID2)
  })


  // Can we join a set of datasources dsIDSet?
  lazy val joinedSet: Constraint[DatasetID] = memoize(args => {
    val dataSpace: DataSpace = args(0).as[DataSpace]

    dataSpace.datasets.toSeq match {

      // No elements
      case Seq() => Seq()

      // One element
      case Seq(dsID1) => Seq(dsID1)

      // Two elements, check joined pair
      case Seq(dsID1, dsID2) => joinedPair(Seq(dataSpace.dimensionSpace, dsID1, dsID2))

      // More than two elements...
      case head +: tail =>

        // joinPair( head, joinSet(tail) )
        val restThenPair = joinedSet(Seq(DataSpace(dataSpace.dimensionSpace, tail.toArray)))
          .flatMap(tailSolution => joinedPair(Seq(dataSpace.dimensionSpace, head, tailSolution)))

        // Set of all joinable pairs between head and some t in tail
        val head2TailPairs = new ArgumentSpace(Seq(dataSpace.dimensionSpace), Seq(head), tail.toSeq).allSolutions(joinedPair)

        // joinSet( joinPair(head, t) +: rest )
        val pairThenRest = head2TailPairs.flatMap(pair => {
          val pairArgs = pair.arguments.map(_.as[DatasetID])
          val rest = dataSpace.datasets.filterNot(pairArgs.contains)
          pair.solutions.flatMap(sol => {
            joinedSet(Seq(DataSpace(dataSpace.dimensionSpace, rest :+ sol)))
          })
        })

        restThenPair ++ pairThenRest
    }
  })

}

case class JoinSpace(dataSpace: DataSpace, queryTarget: ScrubJaySchema) {
  // If set satisfies query target, return the joined set
  def allJoinedDatasets: Seq[DatasetID] = {
    JoinSpace.joinedSet(Seq(dataSpace))
  }
}
