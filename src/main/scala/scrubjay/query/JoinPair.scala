package scrubjay.query

import scrubjay.query.constraintsolver.ConstraintSolver._
import scrubjay.datasetid._
import scrubjay.datasetid.combination.{InterpolationJoin, NaturalJoin}
import scrubjay.datasetid.transformation.{ExplodeList, ExplodeRange}
import scrubjay.schema.{ScrubJayColumnSchema, ScrubJayDimensionSchema}

object JoinPair {

  val interJoinWindow = 60
  val explodePeriod = 30

  def unorderedToPoint(ds: DatasetID, f: ScrubJayColumnSchema): DatasetID = {
    if (f.units.elementType == "MULTIPOINT") {
      ExplodeList(ds, f.name)
    } else {
      ds
    }
  }

  def orderedToPoint(ds: DatasetID, f: ScrubJayColumnSchema): DatasetID = {
    if (f.units.elementType == "RANGE") {
      ExplodeRange(ds, f.name, explodePeriod)
    } else {
      ds
    }
  }

  def allJoinedPairs(dsID1: DatasetID, dsID2: DatasetID, dimensions: Set[ScrubJayDimensionSchema]): Seq[DatasetID] = {
    // Check if domain dimensions match (regardless of units)
    val joinableFields = dsID1.scrubJaySchema
      .joinableFields(dsID2.scrubJaySchema, testUnits = false)
      .map{case (f1, f2) => (f1, f2, dimensions.find(_ == f1.dimension).get)}

    // Explode all possible joinable elements if explodable
    val (ds1Exploded, ds2Exploded) = joinableFields.foldLeft((dsID1, dsID2)){
      // Joinable field is unordered
      case ((ds1, ds2), (f1, f2, ScrubJayDimensionSchema(_, false, false, _))) => {
        (unorderedToPoint(ds1, f1), unorderedToPoint(ds2, f2))
      }
      // Joinable field is ordered
      case ((ds1, ds2), (f1, f2, ScrubJayDimensionSchema(_, true, true, _))) => {
        (orderedToPoint(ds1, f1), orderedToPoint(ds2, f2))
      }
    }

    // If any joinable field is ordered, we must use interpolation join
    if (joinableFields.exists(_._3.ordered)) {
      Seq(
        // Interpolation join is not commutative, return both joinedPair results
        InterpolationJoin(ds1Exploded, ds2Exploded, interJoinWindow),
        InterpolationJoin(ds2Exploded, ds1Exploded, interJoinWindow)
      )
    } else {
      Seq(
        // Natural join is commutative, return one joinedPair result
        NaturalJoin(ds1Exploded, ds2Exploded)
      )
    }
  }

  // Figure out what kind of join to do, then do it
  lazy val joinedPair: Constraint[DatasetID] = memoize(args => {
    val dimensions = args(0).as[Set[ScrubJayDimensionSchema]]
    val dsID1 = args(1).as[DatasetID]
    val dsID2 = args(2).as[DatasetID]
    allJoinedPairs(dsID1, dsID2, dimensions).flatMap(_.asOption)
  })
}
