package testsuite

import scrubjay.datasource._

import scrubjay.transformation.ExplodeDiscreteRange


class ExplodeDiscreteRangeSpec extends ScrubJaySpec {

  lazy val jobQueue: DatasetID = CSVDatasetID(jobQueueFilename, scrubjay.schema.fromJSONFile(jobQueueMetaFilename), Map("header" -> "true", "delimiter" -> "|"))
  lazy val jobQueueExploded: DatasetID = ExplodeDiscreteRange(jobQueue, "nodelist")

  describe("Derive exploded node list") {
    it("should be defined") {
      assert(jobQueueExploded.isValid)
    }
    it("should match ground truth") {
      jobQueueExploded.realize.show()
    }
    it("should have a schema") {
      jobQueueExploded.realize.printSchema()
    }
    it("should pickle/unpickle correctly") {
      assert(DatasetID.fromJsonString(DatasetID.toJsonString(jobQueueExploded)) == jobQueueExploded)
    }
  }
}
