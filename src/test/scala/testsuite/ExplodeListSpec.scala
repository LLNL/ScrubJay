package testsuite

import scrubjay.datasetid._
import scrubjay.dataspace.{DataSpace, DimensionSpace}
import scrubjay.datasetid.transformation.ExplodeList
import scrubjay.schema.ScrubJayDimensionSchema


class ExplodeListSpec extends ScrubJaySpec {

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

  lazy val jobQueueExplodeNodeList: DatasetID = ExplodeList(jobQueue, "domain:node:list")

  describe("Derive exploded node list") {
    it("should be defined") {
      assert(jobQueueExplodeNodeList.isValid(dataSpace.dimensionSpace))
    }
    it("should lookCorrect") {
      println("Before:")
      jobQueue.debugPrint(dataSpace.dimensionSpace)
      println("After:")
      jobQueueExplodeNodeList.debugPrint(dataSpace.dimensionSpace)
    }
    it("should serialize/deserialize correctly") {
      val json: String = DatasetID.toJsonString(jobQueueExplodeNodeList)
      println("JSON:")
      println(json)
      assert(DatasetID.fromJsonString(json) == jobQueueExplodeNodeList)
    }
  }
}
