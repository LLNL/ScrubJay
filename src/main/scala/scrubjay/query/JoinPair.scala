package scrubjay.query

import gov.llnl.ConstraintSolver._
import scrubjay.datasetid._
import scrubjay.dataspace._
import scrubjay.datasetid.combination.{InterpolationJoin, NaturalJoin}

object JoinPair {

  val interJoinWindow = 60
  val explodePeriod = 30000

  // Figure out what kind of join to do, then do it
  lazy val joinedPair: Constraint[DatasetID] = memoize(args => {
    val dimensionSpace = args(0).as[DimensionSpace]
    val dsID1 = args(1).as[DatasetID]
    val dsID2 = args(2).as[DatasetID]

    // Check if domain dimensions match (regardless of units)
    val joinableFields = dsID1.scrubJaySchema(dimensionSpace)
      .joinableFields(dsID2.scrubJaySchema(dimensionSpace), testUnits = false)
      .map{case (f1, f2) => (f1, f2, dimensionSpace.findDimension(f1.dimension).get)}

    joinableFields.map{
      case (f1, f2, Dimension(_, false, false)) => {
        Seq(NaturalJoin(dsID1, dsID2))
      }
      case (f1, f2, Dimension(_, true, true)) => {
        Seq(
          // Not commutative
          InterpolationJoin(dsID1, dsID2, interJoinWindow),
          InterpolationJoin(dsID2, dsID1, interJoinWindow)
        )
      }
    }

    Seq(NaturalJoin(dsID1, dsID2).asOption(dimensionSpace)).flatten
    //UberJoin(dsID1, dsID2)
  })
}
