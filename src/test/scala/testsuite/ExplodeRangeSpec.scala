package testsuite

import scrubjay.datasetid._
import scrubjay.dataspace.{DataSpace, DimensionSpace}
import scrubjay.datasetid.transformation.ExplodeRange
import scrubjay.schema.ScrubJayDimensionSchema


class ExplodeRangeSpec extends ScrubJaySpec {

  lazy val jobQueue: DatasetID = DatasetID.fromJsonFile(jobQueueDatasetIDFilename)

  lazy val dataSpace: DataSpace = DataSpace(
    datasets = Array(
      jobQueue
    )
  )

  lazy val jobQueueExplodeTimeRange: DatasetID = ExplodeRange(jobQueue, "domain:time:range", 30)

  describe("Derive exploded time range") {
    it("should be defined") {
      assert(jobQueueExplodeTimeRange.valid)
    }
    it("should look correct") {
      println("Before:")
      jobQueue.debugPrint
      println("After:")
      jobQueueExplodeTimeRange.debugPrint
    }
    it("should serialize/deserialize correctly") {
      val json: String = DatasetID.toJsonString(jobQueueExplodeTimeRange)
      println("JSON:")
      println(json)
      assert(DatasetID.fromJsonString(json) == jobQueueExplodeTimeRange)
    }
  }
}

