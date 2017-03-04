package testsuite

import scrubjay.datasource._
import scrubjay.metasource._
import scrubjay.derivation.ExplodeList


class ExplodeListSpec extends ScrubJaySpec {

  lazy val jobQueue: DataSourceID = CSVDataSource(jobQueueFilename, CSVMetaSource(jobQueueMetaFilename))
  lazy val jobQueueExploded: DataSourceID = ExplodeList(jobQueue, Seq("nodelist"))

  describe("Derive exploded time span") {
    it("should be defined") {
      assert(jobQueueExploded.isValid)
    }
    it("should match ground truth") {
      assert(jobQueueExploded.realize.collect.toSet == trueJobQueueExploded)
    }
    it("should pickle/unpickle correctly") {
      assert(DataSourceID.fromJsonString(DataSourceID.toJsonString(jobQueueExploded)) == jobQueueExploded)
    }
  }
}
