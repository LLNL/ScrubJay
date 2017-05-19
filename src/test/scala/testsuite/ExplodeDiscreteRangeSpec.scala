package testsuite

import scrubjay.datasetid._
import scrubjay.dataspace.{DataSpace, Dimension, DimensionSpace}
import scrubjay.datasetid.transformation.ExplodeDiscreteRange


class ExplodeDiscreteRangeSpec extends ScrubJaySpec {

  lazy val jobQueue: DatasetID = DatasetID.fromJsonFile(jobQueueDatasetIDFilename)

  lazy val dataSpace: DataSpace = DataSpace(
    dimensionSpace = DimensionSpace(Array(
      Dimension("job", ordered = false, continuous = false),
      Dimension("node", ordered = false, continuous = false),
      Dimension("time", ordered = true, continuous = true))
    ),
    datasets = Array(
      jobQueue
    )
  )

  lazy val jobQueueExplodeNodeList: DatasetID = ExplodeDiscreteRange(jobQueue, "domain:node:list<identifier>")

  describe("Derive exploded node list") {
    it("should be defined") {
      println("Before explode:")
      jobQueue.realize(dataSpace.dimensionSpace).show(false)
      assert(jobQueueExplodeNodeList.isValid(dataSpace.dimensionSpace))
    }
    it("should exist") {
      println("DataFrame:")
      jobQueueExplodeNodeList.realize(dataSpace.dimensionSpace).show(false)
      println("SparkSchema")
      jobQueueExplodeNodeList.realize(dataSpace.dimensionSpace).printSchema()
    }
    it("should serialize/deserialize correctly") {
      val json: String = DatasetID.toJsonString(jobQueueExplodeNodeList)
      println("JSON:")
      println(json)
      assert(DatasetID.fromJsonString(json) == jobQueueExplodeNodeList)
    }
  }
}
