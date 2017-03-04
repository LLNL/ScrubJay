package testsuite

import scrubjay.datasource._
import scrubjay.metasource._

import org.scalactic.source.Position


class CSVDataSourceSpec extends ScrubJaySpec {

  lazy val jobQueueMetaSource: MetaSourceID = CSVMetaSource(jobQueueMetaFilename)
  lazy val jobQueue: DataSourceID = CSVDataSource(jobQueueFilename, jobQueueMetaSource)

  describe("CSV sourced job queue data") {
    it("should match ground truth") {
      assert(jobQueue.realize.collect.toSet == trueJobQueue)
    }
    it("should pickle/unpickle correctly") {
      assert(DataSourceID.fromJsonString(DataSourceID.toJsonString(jobQueue)) == jobQueue)
    }
  }

}
