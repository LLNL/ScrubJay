package testsuite

import scrubjay.dataset._
import scrubjay.dataspace.{DataSpace, DimensionSpace}
import scrubjay.dataset.transformation.ExplodeDiscreteRange


class ExplodeDiscreteRangeSpec extends ScrubJaySpec {

  lazy val jobQueue: DatasetID = DatasetID.fromJsonFile(jobQueueDatasetIDFilename)
  lazy val jobQueueExplodeNodeList: DatasetID = new ExplodeDiscreteRange(jobQueue, "nodelist")

  describe("Derive exploded node list") {
    it("should be defined") {
      assert(jobQueueExplodeNodeList.isValid)
    }
    it("should exist") {
      println("DataFrame:")
      jobQueueExplodeNodeList.realize.show(false)
      println("Schema")
      println(jobQueueExplodeNodeList.realize.schema.prettyJson)
    }
    it("should pickle/unpickle correctly") {
      val json: String = DatasetID.toJsonString(jobQueueExplodeNodeList)
      println("JSON:")
      println(json)
      assert(DatasetID.fromJsonString(json) == jobQueueExplodeNodeList)
    }
  }
}
