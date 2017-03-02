package testsuite

import scrubjay._

import org.scalactic.source.Position

class LocalDataSourceSpec extends ScrubJaySpec {

  lazy val jobQueue: Option[DataSourceID] = sc.createLocalDataSource(jobQueueRawData, jobQueueMeta.keySet.toSeq, jobQueueMeta)
  lazy val cabLayout: Option[DataSourceID] = sc.createLocalDataSource(clusterLayoutRawData, clusterLayoutMeta.keySet.toSeq, clusterLayoutMeta)

  describe("Locally generated job queue data") {
    it("should be defined") {
      assert(jobQueue.isDefined)
    }
    it("should match ground truth") {
      assert(jobQueue.get.realize.collect.toSet == trueJobQueue)
    }
  }

  describe("Locally generated cab layout data") {
    it("should be defined") {
      assert(cabLayout.isDefined)
    }
    it("should match ground truth") {
      assert(cabLayout.get.realize.collect.toSet == trueCabLayout)
    }
  }
}
