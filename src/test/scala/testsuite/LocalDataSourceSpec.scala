package testsuite

import scrubjay._

import org.scalactic.source.Position

class LocalDataSourceSpec extends ScrubJaySpec {

  describe("LocalDataSource") {

    lazy val jobQueue = sc.createLocalDataSource(jobQueueRawData, jobQueueMeta.keySet.toSeq, new MetaSource(jobQueueMeta))
    lazy val cabLayout = sc.createLocalDataSource(clusterLayoutRawData, clusterLayoutMeta.keySet.toSeq, new MetaSource(clusterLayoutMeta))

    describe("Locally generated job queue data") {
      it("should be defined") {
        assert(jobQueue.isDefined)
      }
      it("should match ground truth") {
        assert(jobQueue.get.rdd.collect.toSet == trueJobQueue)
      }
    }

    describe("Locally generated cab layout data") {
      it("should be defined") {
        assert(cabLayout.isDefined)
      }
      it("should match ground truth") {
        assert(cabLayout.get.rdd.collect.toSet == trueCabLayout)
      }
    }

  }
}
