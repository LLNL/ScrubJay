package testsuite

import scrubjay.datasetid._

class CSVDatasetIDSpec extends ScrubJaySpec {

  lazy val jobQueue: DatasetID = DatasetID.fromJsonFile(jobQueueDatasetIDFilename)

  describe("CSV sourced job queue data") {
    it("should exist") {
      println("DataFrame:")
      jobQueue.realize.show(false)
      println("SparkSchema")
      jobQueue.realize.printSchema()
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
