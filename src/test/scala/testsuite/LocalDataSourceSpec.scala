package testsuite

import scrubjay.datasource.{DataSourceID, LocalDataSource}
import scrubjay.metasource.LocalMetaSource

import org.scalactic.source.Position


class LocalDataSourceSpec extends ScrubJaySpec {

  lazy val jobQueue: DataSourceID = LocalDataSource(jobQueueRawData, LocalMetaSource(jobQueueMeta))

  describe("Locally generated job queue data") {
    it("should be defined") {
      assert(jobQueue.isValid)
    }
    it("should match ground truth") {
      assert(jobQueue.realize.collect.toSet == trueJobQueue)
    }
    it("should pickle/unpickle correctly") {
      assert(DataSourceID.fromJsonString(DataSourceID.toJsonString(jobQueue)) == jobQueue)
    }
  }
}
