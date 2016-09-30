package testsuite

import scrubjay._
import scrubjay.meta._
import scrubjay.datasource._

import scrubjay.meta.LocalMetaSource._
import scrubjay.datasource.LocalDataSource._

import scrubjay.query._

import org.scalatest.{BeforeAndAfterAll, FunSpec}


object QuerySpec {

  def createDataSources(sjs: ScrubJaySession): Set[DataSource] = {
    Set(
      sjs.createLocalDataSource(cabLayoutColumns, cabLayoutRawData, createLocalMetaSource(cabLayoutMeta)),
      sjs.createLocalDataSource(jobQueueColumns, jobQueueRawData, createLocalMetaSource(jobQueueMeta))
    )
  }

  def createSingleSourceQueryMetaEntries: Set[MetaEntry] = {
    Set(
      MetaEntry.fromStringTuple("job", "job", "identifier"),
      MetaEntry.fromStringTuple("duration", "time", "seconds")
    )
  }

  // TODO: multiple source query test

}

class QuerySpec extends FunSpec with BeforeAndAfterAll {

  val sjs: ScrubJaySession = new ScrubJaySession()

  override protected def afterAll {
    sjs.sc.stop()
  }

  describe("Query with single datasource solution") {

    lazy val query = new Query(sjs, QuerySpec.createDataSources(sjs), QuerySpec.createSingleSourceQueryMetaEntries)
    lazy val solutions = query.run.toList

    it("should have a single solution") {
      assert(solutions.length == 1)
    }

    it("should find the correct datasource") {
      assert(solutions.head.rdd.collect.toSet == trueJobQueue)
    }
  }
}
