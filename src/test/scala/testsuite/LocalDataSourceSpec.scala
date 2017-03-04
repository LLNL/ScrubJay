package testsuite

import scrubjay.datasource.{DataSourceID, LocalDataSource}
import scrubjay.metasource.LocalMetaSource

import org.scalactic.source.Position


class LocalDataSourceSpec extends ScrubJaySpec {

  val jobQueueData = Seq(
    Map(
      "jobid"    -> "123",
      "nodelist" -> "1,2,3",
      "elapsed"  -> "23",
      "timespan" -> "2016-08-11T3:30:00+0000,2016-08-11T3:31:00+0000"
    ),
    Map(
      "jobid"    -> 456,
      "nodelist" -> List(4, 5, 6),
      "elapsed"  -> 45,
      "timespan" -> "2016-08-11T3:30:00+0000,2016-08-11T3:32:00+0000"
    )
  )

  val jobQueueMeta = Seq(
    ("jobid", "domain", "job", "identifier"),
    ("nodelist", "domain", "node", "list<identifier>"),
    ("elapsed", "value", "time", "seconds"),
    ("timespan", "domain", "time", "datetimespan")
  )

  lazy val jobQueue: DataSourceID = LocalDataSource(jobQueueData, LocalMetaSource(jobQueueMeta))

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
