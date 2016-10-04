package testsuite

import scrubjay._
import scrubjay.meta._
import scrubjay.datasource._

import scrubjay.metasource.LocalMetaSource._
import scrubjay.datasource.LocalDataSource._

import scrubjay.query._

import org.scalatest.{BeforeAndAfterAll, FunSpec}


object QuerySpec {

  def createDataSources(sjs: ScrubJaySession): Set[DataSource] = {
    Set(
      sjs.createLocalDataSource(clusterLayoutColumns, clusterLayoutRawData, createLocalMetaSource(clusterLayoutMeta)),
      sjs.createLocalDataSource(nodeDataColumns, nodeDataRawData, createLocalMetaSource(nodeDataMeta)),
      sjs.createLocalDataSource(jobQueueColumns, jobQueueRawData, createLocalMetaSource(jobQueueMeta))
    )
  }

  def createSingleSourceQueryMetaEntries: Set[MetaEntry] = {
    Set(
      MetaEntry.fromStringTuple("job", "job", "identifier"),
      MetaEntry.fromStringTuple("duration", "time", "seconds")
    )
  }

  def createMultipleSourceQueryMetaEntries: Set[MetaEntry] = {
    Set(
      MetaEntry.fromStringTuple("rack", "rack", "identifier"),
      MetaEntry.fromStringTuple("cumulative", "flops", "count")
    )
  }

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

  describe("Query with multiple datasources solution") {

    lazy val query = new Query(sjs, QuerySpec.createDataSources(sjs), QuerySpec.createMultipleSourceQueryMetaEntries)
    lazy val solutions = query.run.toList

    it("should have a multiple solution") {
      assert(solutions.length == 1)
    }

    it("should find the correct datasource") {
      assert(solutions.head.rdd.collect.toSet == trueNodeDataJoinedWithClusterLayout)
    }
  }
}
