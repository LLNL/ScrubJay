package testsuite

import scrubjay._


class DeriveTimeSpanExplodeListSpec extends ScrubJaySpec {

  lazy val jobQueue: Option[DataSourceID] = sc.createLocalDataSource(jobQueueRawData, jobQueueMeta.keySet.toSeq, jobQueueMeta)
  lazy val jobQueueSpan: Option[DataSourceID] = jobQueue.get.deriveTimeSpan

  describe("Derive time span") {
    it("should be defined") {
      assert(jobQueueSpan.isDefined)
    }
    it("should match ground truth") {
      assert(jobQueueSpan.get.realize.collect.toSet == trueJobQueueSpan)
    }
  }

  lazy val jobQueueSpanExploded: Option[DataSourceID] = jobQueueSpan.get.deriveExplodeList(Seq("nodelist"))

  describe("Derive exploded time span") {
    it("should be defined") {
      assert(jobQueueSpanExploded.isDefined)
    }
    it("should match ground truth") {
      assert(jobQueueSpanExploded.get.realize.collect.toSet == trueJobQueueSpanExploded)
    }
  }
}
