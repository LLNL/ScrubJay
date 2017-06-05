package testsuite

import scrubjay.datasetid._
import scrubjay.dataspace.{Dimension, DimensionSpace}

class CSVDatasetIDSpec extends ScrubJaySpec {

  lazy val jobQueue: DatasetID = DatasetID.fromJsonFile(jobQueueDatasetIDFilename)
  val dimensionSpace = DimensionSpace(Array(
    Dimension("job", ordered = false, continuous = false),
    Dimension("node", ordered = false, continuous = false),
    Dimension("time", ordered = true, continuous = true))
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
