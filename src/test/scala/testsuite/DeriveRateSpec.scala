package testsuite

import scrubjay.datasetid._
import scrubjay.dataspace.{DataSpace, Dimension, DimensionSpace}
import scrubjay.datasetid.transformation.DeriveRate


class DeriveRateSpec extends ScrubJaySpec {

  lazy val dimensionSpace: DimensionSpace =
    DimensionSpace(Array(
      Dimension("node", ordered = false, continuous = false),
      Dimension("flops", ordered = true, continuous = true),
      Dimension("time", ordered = true, continuous = true))
    )

  lazy val nodeFlops: DatasetID = DatasetID.fromJsonFile(nodeFlopsDatasetIDFilename)

  lazy val jobQueueExplodeNodeList: DatasetID = DeriveRate(nodeFlops, "flops", "time", 2)

  describe("Derive exploded node list") {
    it("should be defined") {
      assert(jobQueueExplodeNodeList.isValid(dimensionSpace))
    }
    it("should lookCorrect") {
      println("Before:")
      nodeFlops.debugPrint(dimensionSpace)
      println("After:")
      jobQueueExplodeNodeList.debugPrint(dimensionSpace)
    }
    it("should serialize/deserialize correctly") {
      val json: String = DatasetID.toJsonString(jobQueueExplodeNodeList)
      println("JSON:")
      println(json)
      assert(DatasetID.fromJsonString(json) == jobQueueExplodeNodeList)
    }
  }
}
