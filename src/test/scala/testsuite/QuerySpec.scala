package testsuite

import scrubjay._

import org.apache.spark._

import org.scalatest._
import org.scalactic.source.Position


object QuerySpec {

  def createDataSources(sc: SparkContext): Set[DataSource] = {
    Set(
      sc.createLocalDataSource(clusterLayoutRawData, clusterLayoutColumns, createLocalMetaSource(clusterLayoutMeta)),
      sc.createLocalDataSource(nodeDataRawData, nodeDataColumns, createLocalMetaSource(nodeDataMeta)),
      sc.createLocalDataSource(jobQueueRawData, jobQueueColumns, createLocalMetaSource(jobQueueMeta))
    ).flatten
  }

  def createSingleSourceQueryMetaEntries: Set[MetaEntry] = {
    Set(
      metaEntryFromStrings("job", "job", "identifier"),
      metaEntryFromStrings("duration", "time", "seconds")
    )
  }

  def createMultipleSourceQueryMetaEntries: Set[MetaEntry] = {
    Set(
      metaEntryFromStrings("rack", "rack", "identifier"),
      metaEntryFromStrings("cumulative", "flops", "count")
    )
  }

}

class QuerySpec extends FunSpec with BeforeAndAfterAll {

  var sc: SparkContext = _

  override protected def beforeAll {
    sc = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("ScrubJayTest"))
  }

  override protected def afterAll {
    sc.stop()
  }

  describe("Query with single datasource solution") {

    lazy val query = new Query(QuerySpec.createDataSources(sc), QuerySpec.createSingleSourceQueryMetaEntries)
    lazy val solutions = query.run.toList

    it("should have a single solution") {
      assert(solutions.length == 1)
    }

    it("should find the correct datasource") {
      assert(solutions.head.rdd.collect.toSet == trueJobQueue)
    }
  }

  describe("Query with multiple datasources solution") {

    lazy val query = new Query(QuerySpec.createDataSources(sc), QuerySpec.createMultipleSourceQueryMetaEntries)
    lazy val solutions = query.run.toList

    it("should have a multiple solution") {
      assert(solutions.length == 1)
    }

    it("should find the correct datasource") {
      assert(solutions.head.rdd.collect.toSet == trueNodeDataJoinedWithClusterLayout)
    }
  }
}
