package testsuite

import scrubjay._


class DeriveTimeSpanExplodeListSpec extends ScrubJaySpec {

  lazy val jobQueue: Option[ScrubJayRDD] = sc.createLocalDataSource(jobQueueRawData, jobQueueMeta.keySet.toSeq, new MetaSource(jobQueueMeta))
  lazy val jobQueueSpan: Option[ScrubJayRDD] = jobQueue.get.deriveTimeSpan

  describe("Derive Time Span") {
    it("should be defined") {
      assert(jobQueueSpan.isDefined)
    }
    it("should match ground truth") {
      assert(jobQueueSpan.get.rdd.collect.toSet == trueJobQueueSpan)
    }
  }

  lazy val jobQueueSpanExploded: Option[ScrubJayRDD] = jobQueueSpan.get.deriveExplodeList(Seq("nodelist"))

  describe("...with exploded node list") {
    it("should be defined") {
      assert(jobQueueSpanExploded.isDefined)
    }
    it("should match ground truth") {
      assert(jobQueueSpanExploded.get.rdd.collect.toSet == trueJobQueueSpanExploded)
    }
  }
}
