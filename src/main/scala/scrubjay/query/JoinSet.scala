// Copyright 2018 Lawrence Livermore National Security, LLC and other
// ScrubJay Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: (Apache-2.0 OR MIT)

package scrubjay.query

import scrubjay.datasetid._
import scrubjay.dataspace.DataSpace

import scrubjay.query.constraintsolver.ConstraintSolver._

object JoinSet {

  // Can we join a set of datasources dsIDSet?
  lazy val joinedSet: Constraint[DatasetID] = memoize(args => {
    val dataSpace: DataSpace = args(0).as[DataSpace]

    dataSpace.datasets.toSeq match {

      // No elements
      case Seq() => Seq()

      // One element
      case Seq(dsID1) => Seq(dsID1)

      // Two elements, check joined pair
      case Seq(dsID1, dsID2) => JoinPair.joinedPair(Seq(dataSpace.dimensions, dsID1, dsID2))

      // More than two elements...
      case head +: tail =>

        // joinPair( head, joinSet(tail) )
        val restThenPair = joinedSet(Seq(DataSpace(tail.toArray)))
          .flatMap(tailSolution => JoinPair.joinedPair(Seq(dataSpace.dimensions, head, tailSolution)))

        // Set of all joinable pairs between head and some t in tail
        val head2TailPairs = new ArgumentSpace(Seq(dataSpace.dimensions), Seq(head), tail.toSeq).allSolutions(JoinPair.joinedPair)

        // joinSet( joinPair(head, t) +: rest )
        val pairThenRest = head2TailPairs.flatMap(pair => {
          val pairArgs = pair.arguments.tail.map(_.as[DatasetID])
          val rest = dataSpace.datasets.filterNot(pairArgs.contains)
          pair.solutions.flatMap(sol => {
            joinedSet(Seq(DataSpace(rest :+ sol)))
          })
        })

        restThenPair ++ pairThenRest
    }
  })

}
