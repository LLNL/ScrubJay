package testsuite

import scrubjay.datasetid._
import scrubjay.dataspace.{DataSpace, DimensionSpace}
import scrubjay.datasetid.transformation.ExplodeRange
import scrubjay.schema.ScrubJayDimensionSchema


class ExplodeRangeSpec extends ScrubJaySpec {

  lazy val jobQueue: DatasetID = DatasetID.fromJsonFile(jobQueueDatasetIDFilename)

  lazy val dataSpace: DataSpace = DataSpace(
    dimensionSpace = DimensionSpace(Array(
      ScrubJayDimensionSchema("job", ordered = false, continuous = false),
      ScrubJayDimensionSchema("node", ordered = false, continuous = false),
      ScrubJayDimensionSchema("time", ordered = true, continuous = true))
    ),
    datasets = Array(
      jobQueue
    )
  )

  lazy val jobQueueExplodeTimeRange: DatasetID = ExplodeRange(jobQueue, "domain:time:range", 30)

  describe("Derive exploded time range") {
    it("should be defined") {
      assert(jobQueueExplodeTimeRange.isValid(dataSpace.dimensionSpace))
    }
    it("should look correct") {
      println("Before:")
      jobQueue.debugPrint(dataSpace.dimensionSpace)
      println("After:")
      jobQueueExplodeTimeRange.debugPrint(dataSpace.dimensionSpace)
    }
    it("should serialize/deserialize correctly") {
      val json: String = DatasetID.toJsonString(jobQueueExplodeTimeRange)
      println("JSON:")
      println(json)
      assert(DatasetID.fromJsonString(json) == jobQueueExplodeTimeRange)
    }
  }
}

