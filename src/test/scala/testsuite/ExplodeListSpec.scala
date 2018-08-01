package testsuite

import scrubjay.datasetid._
import scrubjay.dataspace.{DataSpace, DimensionSpace}
import scrubjay.datasetid.transformation.ExplodeList
import scrubjay.schema.ScrubJayDimensionSchema


class ExplodeListSpec extends ScrubJaySpec {

  lazy val jobQueue: DatasetID = DatasetID.fromJsonFile(jobQueueDatasetIDFilename)

  lazy val dataSpace: DataSpace = DataSpace(
    datasets = Array(
      jobQueue
    )
  )

  lazy val jobQueueExplodeNodeList: DatasetID = ExplodeList(jobQueue, "domain:node:list")

  describe("Derive exploded node list") {
    it("should be defined") {
      assert(jobQueueExplodeNodeList.valid)
    }
    it("should lookCorrect") {
      println("Before:")
      jobQueue.debugPrint
      println("After:")
      jobQueueExplodeNodeList.debugPrint
    }
    it("should serialize/deserialize correctly") {
      val json: String = DatasetID.toJsonString(jobQueueExplodeNodeList)
      println("JSON:")
      println(json)
      assert(DatasetID.fromJsonString(json) == jobQueueExplodeNodeList)
    }
  }
}
