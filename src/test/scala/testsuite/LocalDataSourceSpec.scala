package testsuite

import scrubjay._

import org.scalactic.source.Position

class LocalDataSourceSpec extends ScrubJaySpec {

  lazy val jobQueue: DataSourceID = sc.createLocalDataSource(jobQueueRawData, jobQueueMeta.keySet.toSeq, jobQueueMeta)
  lazy val cabLayout: DataSourceID = sc.createLocalDataSource(clusterLayoutRawData, clusterLayoutMeta.keySet.toSeq, clusterLayoutMeta)

  describe("Locally generated job queue data") {
    it("should be defined") {
      assert(jobQueue.isValid)
    }
    it("should match ground truth") {
      assert(jobQueue.realize.collect.toSet == trueJobQueue)
    }
  }

  describe("Locally generated cab layout data") {
    it("should be defined") {
      assert(cabLayout.isValid)
    }
    it("should match ground truth") {
      assert(cabLayout.realize.collect.toSet == trueCabLayout)
    }
  }
}
