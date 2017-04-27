package testsuite

import scrubjay.datasetid._
import scrubjay.dataspace.{DataSpace, DimensionSpace}
import scrubjay.datasetid.transformation.ExplodeDiscreteRange


class ExplodeDiscreteRangeSpec extends ScrubJaySpec {

  lazy val jobQueue: DatasetID = DatasetID.fromJsonFile(jobQueueDatasetIDFilename)

  lazy val dataSpace: DataSpace = DataSpace(
    dimensions = Array(
      DimensionSpace("job", ordered = false, continuous = false),
      DimensionSpace("node", ordered = false, continuous = false),
      DimensionSpace("time", ordered = true, continuous = true)),
    datasets = Array(
      jobQueue
    )
  )

  lazy val jobQueueExplodeNodeList: DatasetID = ExplodeDiscreteRange(jobQueue, "nodelist")

  describe("Derive exploded node list") {
    it("should be defined") {
      assert(jobQueueExplodeNodeList.isValid)
    }
    it("should exist") {
      println("DataFrame:")
      jobQueueExplodeNodeList.realize.show(false)
      println("SparkSchema")
      jobQueueExplodeNodeList.realize.printSchema()
    }
    it("should serialize/deserialize correctly") {
      val json: String = DatasetID.toJsonString(jobQueueExplodeNodeList)
      println("JSON:")
      println(json)
      assert(DatasetID.fromJsonString(json) == jobQueueExplodeNodeList)
    }
  }
}
