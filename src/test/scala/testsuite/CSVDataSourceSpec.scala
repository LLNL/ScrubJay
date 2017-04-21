package testsuite

import org.apache.spark.sql.types.StructType
import scrubjay.dataset._

import org.scalactic.source.Position


class CSVDataSourceSpec extends ScrubJaySpec {

  lazy val jobQueueMetaSource: StructType = scrubjay.schema.fromJSONFile(jobQueueMetaFilename)
  lazy val jobQueue: DatasetID = CSVDatasetID(jobQueueFilename, jobQueueMetaSource, Map("header" -> "true", "delimiter" -> "|"))

  describe("CSV sourced job queue data") {
    it("should exist") {
      println("DataFrame:")
      jobQueue.realize.show(false)
      println("Schema:")
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
