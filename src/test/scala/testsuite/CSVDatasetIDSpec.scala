package testsuite

import scrubjay.datasetid._
import scrubjay.dataspace.DimensionSpace
import scrubjay.schema.ScrubJayDimensionSchema

class CSVDatasetIDSpec extends ScrubJaySpec {

  lazy val jobQueue: DatasetID = DatasetID.fromJsonFile(jobQueueDatasetIDFilename)
  val dimensionSpace = DimensionSpace(Array(
    ScrubJayDimensionSchema("job", ordered = false, continuous = false),
    ScrubJayDimensionSchema("node", ordered = false, continuous = false),
    ScrubJayDimensionSchema("time", ordered = true, continuous = true))
  )

  describe("CSV sourced job queue data") {
    it("should exist") {
      jobQueue.debugPrint(dimensionSpace)
    }
    it("should serialize/deserialize") {
      val json: String = DatasetID.toJsonString(jobQueue)
      println("JSON:")
      println(json)
      val reloaded: DatasetID = DatasetID.fromJsonString(json)
      assert(reloaded == jobQueue)
    }
  }
}
