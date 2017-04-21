package testsuite

import org.apache.spark.sql.types.StructType
import scrubjay.datasource._
import scrubjay.schema._
import org.scalactic.source.Position


class CSVDataSourceSpec extends ScrubJaySpec {

  lazy val jobQueueMetaSource: StructType = scrubjay.schema.fromJSONFile(jobQueueMetaFilename)
  lazy val jobQueue: DatasetID = CSVDatasetID(jobQueueFilename, jobQueueMetaSource, Map("header" -> "true", "delimiter" -> "|"))

  describe("CSV sourced job queue data") {
    it("should exist") {
      jobQueue.realize.collect()
    }
    it("should serialize/deserialize") {
      val json: String = DatasetID.toJsonString(jobQueue)
      val reloaded: DatasetID = DatasetID.fromJsonString(json)
      assert(reloaded == jobQueue)
    }
  }
}
