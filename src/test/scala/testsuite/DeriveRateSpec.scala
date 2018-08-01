package testsuite

import scrubjay.datasetid._
import scrubjay.dataspace.{DataSpace, DimensionSpace}
import scrubjay.datasetid.transformation.DeriveRate
import scrubjay.schema.ScrubJayDimensionSchema


class DeriveRateSpec extends ScrubJaySpec {

  lazy val dimensionSpace: DimensionSpace =
    DimensionSpace(Array(
      ScrubJayDimensionSchema("node", ordered = false, continuous = false),
      ScrubJayDimensionSchema("flops", ordered = true, continuous = true),
      ScrubJayDimensionSchema("time", ordered = true, continuous = true))
    )

  lazy val nodeFlops: DatasetID = DatasetID.fromJsonFile(nodeFlopsDatasetIDFilename)

  lazy val jobQueueExplodeNodeList: DatasetID = DeriveRate(nodeFlops, "flops", "time", 2)

  describe("Derive exploded node list") {
    it("should be defined") {
      assert(jobQueueExplodeNodeList.valid)
    }
    it("should lookCorrect") {
      println("Before:")
      nodeFlops.debugPrint
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
