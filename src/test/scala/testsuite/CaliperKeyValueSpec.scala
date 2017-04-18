package testsuite

import scrubjay.datasource._
import scrubjay.metasource._

import org.scalactic.source.Position


class CaliperKeyValueSpec extends ScrubJaySpec {

  val threadtraceFilename: String = getClass.getResource("/threadtrace.ckv").getPath

  //lazy val jobQueueMetaSource: MetaSourceID = CSVMetaSource(jobQueueMetaFilename)
  lazy val threadtrace: DataSourceID = new CaliperKeyValueDataSource(threadtraceFilename, MetaSourceID.empty)

  describe("CSV sourced job queue data") {
    it("should match ground truth") {
      threadtrace.realize
      //assert(jobQueue.realize.collect.toSet == trueJobQueue)
    }
    it("should pickle/unpickle correctly") {
      //assert(DataSourceID.fromJsonString(DataSourceID.toJsonString(jobQueue)) == jobQueue)
    }
  }

}
