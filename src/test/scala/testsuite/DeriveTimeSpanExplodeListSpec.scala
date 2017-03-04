package testsuite

import scrubjay.datasource._
import scrubjay.metasource._

import scrubjay.derivation.{ExplodeList, TimeSpan}


class DeriveTimeSpanExplodeListSpec extends ScrubJaySpec {

  lazy val jobQueue: DataSourceID = CSVDataSource(jobQueueFilename, CSVMetaSource(jobQueueMetaFilename))
  lazy val jobQueueSpan: DataSourceID = TimeSpan(jobQueue)

  describe("Derive time span") {
    it("should be defined") {
      assert(jobQueueSpan.isValid)
    }
    it("should match ground truth") {
      assert(jobQueueSpan.realize.collect.toSet == trueJobQueueSpan)
    }
    it("should pickle/unpickle correctly") {
      assert(DataSourceID.fromJsonString(DataSourceID.toJsonString(jobQueueSpan)) == jobQueueSpan)
    }
  }

  lazy val jobQueueSpanExploded: DataSourceID = ExplodeList(jobQueueSpan, Seq("nodelist"))

  describe("Derive exploded time span") {
    it("should be defined") {
      assert(jobQueueSpanExploded.isValid)
    }
    it("should match ground truth") {
      assert(jobQueueSpanExploded.realize.collect.toSet == trueJobQueueSpanExploded)
    }
    it("should pickle/unpickle correctly") {
      assert(DataSourceID.fromJsonString(DataSourceID.toJsonString(jobQueueSpanExploded)) == jobQueueSpanExploded)
    }
  }
}
