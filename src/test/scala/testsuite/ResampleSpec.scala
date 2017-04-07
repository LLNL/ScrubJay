package testsuite

import scrubjay.datasource._
import scrubjay.metasource._

import org.scalactic.source.Position


class ResampleSpec extends ScrubJaySpec {


  val resampleDataFilename: String = getClass.getResource("/resampleTest.csv").getPath
  val resampleMetaFilename: String = getClass.getResource("/resampleTestMeta.csv").getPath

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

