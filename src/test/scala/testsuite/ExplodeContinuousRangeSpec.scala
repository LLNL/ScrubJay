package testsuite

import scrubjay.datasource._
import scrubjay.metasource._
import scrubjay.derivation.ExplodeContinuousRange


class ExplodeContinuousRangeSpec extends ScrubJaySpec {

  lazy val jobQueue: DataSourceID = CSVDataSource(jobQueueFilename, CSVMetaSource(jobQueueMetaFilename))
  lazy val jobQueueExploded: DataSourceID = ExplodeContinuousRange(jobQueue, "timespan", 1000)

  describe("Derive exploded node list") {
    it("should be defined") {
      assert(jobQueueExploded.isValid)
    }
    it("should match ground truth") {
      assert(jobQueueExploded.realize.collect.toSet == trueJobQueueExplodedList)
    }
    it("should pickle/unpickle correctly") {
      assert(DataSourceID.fromJsonString(DataSourceID.toJsonString(jobQueueExploded)) == jobQueueExploded)
    }
  }
}

