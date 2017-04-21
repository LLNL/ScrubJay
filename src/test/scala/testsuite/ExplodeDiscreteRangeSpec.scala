package testsuite

import scrubjay.dataset._

import scrubjay.transformation.ExplodeDiscreteRange


class ExplodeDiscreteRangeSpec extends ScrubJaySpec {

  lazy val jobQueue: DatasetID = CSVDatasetID(jobQueueFilename, scrubjay.schema.fromJSONFile(jobQueueMetaFilename), Map("header" -> "true", "delimiter" -> "|"))
  lazy val jobQueueExploded: DatasetID = ExplodeDiscreteRange(jobQueue, "nodelist")

  describe("Derive exploded node list") {
    it("should be defined") {
      assert(jobQueueExploded.isValid)
    }
    it("should exist") {
      println("DataFrame:")
      jobQueueExploded.realize.show(false)
      println("Schema")
      jobQueueExploded.realize.printSchema()
    }
    it("should pickle/unpickle correctly") {
      val json: String = DatasetID.toJsonString(jobQueueExploded)
      println("JSON:")
      println(json)
      assert(DatasetID.fromJsonString(json) == jobQueueExploded)
    }
  }
}
