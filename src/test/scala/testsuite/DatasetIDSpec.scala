package testsuite

import org.apache.spark.sql.types.StructType
import scrubjay.datasetid.DatasetID
import scrubjay.datasetid.original.CSVDatasetID

class DatasetIDSpec extends ScrubJaySpec {

  lazy val dsID: DatasetID = DatasetID.fromJsonFile(jobQueueDatasetIDFilename)

  describe("Load ScrubJay DatasetID from .sj file") {
    it("should match ground truth") {
      // TODO
    }
  }
}
