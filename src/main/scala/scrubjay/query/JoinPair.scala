package scrubjay.query

import gov.llnl.ConstraintSolver._
import scrubjay.datasetid._
import scrubjay.dataspace._
import scrubjay.datasetid.combination.{InterpolationJoin, NaturalJoin}
import scrubjay.datasetid.transformation.{ExplodeRange, ExplodeList}

object JoinPair {

  val interJoinWindow = 60
  val explodePeriod = 30

  def unorderedToPoint(ds: DatasetID, f: ScrubJayField): DatasetID = {
    if (f.units.elementType == "MULTIPOINT") {
      ExplodeList(ds, f.name)
    } else {
      ds
    }
  }

  def orderedToPoint(ds: DatasetID, f: ScrubJayField): DatasetID = {
    if (f.units.elementType == "RANGE") {
      ExplodeRange(ds, f.name, explodePeriod)
    } else {
      ds
    }
  }

  def allJoinedPairs(dsID1: DatasetID, dsID2: DatasetID, dimensionSpace: DimensionSpace): Seq[DatasetID] = {
    // Check if domain dimensions match (regardless of units)
    val joinableFields = dsID1.scrubJaySchema(dimensionSpace)
      .joinableFields(dsID2.scrubJaySchema(dimensionSpace), testUnits = false)
      .map{case (f1, f2) => (f1, f2, dimensionSpace.findDimension(f1.dimension).get)}

    // Explode all possible joinable elements
    val (ds1Exploded, ds2Exploded) = joinableFields.foldLeft((dsID1, dsID2)){
      // Joinable field is unordered
      case ((ds1, ds2), (f1, f2, Dimension(_, false, false))) => {
        (unorderedToPoint(ds1, f1), unorderedToPoint(ds2, f2))
      }
      // Joinable field is ordered
      case ((ds1, ds2), (f1, f2, Dimension(_, true, true))) => {
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
    val dimensionSpace = args(0).as[DimensionSpace]
    val dsID1 = args(1).as[DatasetID]
    val dsID2 = args(2).as[DatasetID]
    allJoinedPairs(dsID1, dsID2, dimensionSpace).flatMap(_.asOption(dimensionSpace))
  })
}
