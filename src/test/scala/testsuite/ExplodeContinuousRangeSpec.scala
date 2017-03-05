package testsuite

import scrubjay.datasource._
import scrubjay.metasource._
import scrubjay.derivation.ExplodeContinuousRange


class ExplodeContinuousRangeSpec extends ScrubJaySpec {

  lazy val jobQueue: DataSourceID = CSVDataSource(jobQueueFilename, CSVMetaSource(jobQueueMetaFilename))
  lazy val jobQueueExplodedTime: DataSourceID = ExplodeContinuousRange(jobQueue, "timespan", 60000)

  describe("Derive exploded node list") {
    it("should be defined") {
      assert(jobQueueExplodedTime.isValid)
    }
    it("should match ground truth") {
      assert(jobQueueExplodedTime.realize.collect.toSet == trueJobQueueExplodedTime)
    }
    it("should pickle/unpickle correctly") {
      assert(DataSourceID.fromJsonString(DataSourceID.toJsonString(jobQueueExplodedTime)) == jobQueueExplodedTime)
    }
  }
}

