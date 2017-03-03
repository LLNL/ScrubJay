package testsuite

import scrubjay._


class DeriveTimeSpanExplodeListSpec extends ScrubJaySpec {

  lazy val jobQueue: DataSourceID = sc.createLocalDataSource(jobQueueRawData, jobQueueMeta.keySet.toSeq, jobQueueMeta)
  lazy val jobQueueSpan: DataSourceID = jobQueue.deriveTimeSpan

  describe("Derive time span") {
    it("should be defined") {
      assert(jobQueueSpan.isValid)
    }
    it("should match ground truth") {
      assert(jobQueueSpan.realize.collect.toSet == trueJobQueueSpan)
    }
  }

  lazy val jobQueueSpanExploded: DataSourceID = jobQueueSpan.deriveExplodeList(Seq("nodelist"))

  describe("Derive exploded time span") {
    it("should be defined") {
      assert(jobQueueSpanExploded.isValid)
    }
    it("should match ground truth") {
      assert(jobQueueSpanExploded.realize.collect.toSet == trueJobQueueSpanExploded)
    }
  }
}
