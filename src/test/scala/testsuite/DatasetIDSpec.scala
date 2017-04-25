package testsuite

import org.apache.spark.sql.types.StructType
import scrubjay.dataset.DatasetID
import scrubjay.dataset.original.CSVDatasetID

class DatasetIDSpec extends ScrubJaySpec {

  lazy val dsID: DatasetID = DatasetID.fromJsonFile(jobQueueDatasetIDFilename)

  lazy val jobQueueMetaSource: StructType = scrubjay.schema.loadFromJSONFile(jobQueueMetaFilename)
  lazy val jobQueue: DatasetID = CSVDatasetID(jobQueueFilename, jobQueueMetaSource, Map("header" -> "true", "delimiter" -> "|"))

  describe("Load ScrubJay DatasetID from .sj file") {
    it("should match ground truth") {
      assert(dsID.realize.collect.toSet == jobQueue.realize.collect.toSet)
    }
  }
}
