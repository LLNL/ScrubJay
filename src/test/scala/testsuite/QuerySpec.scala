package testsuite

import scrubjay._
import org.apache.spark.SparkContext
import org.scalactic.source.Position
import scrubjay.datasource.DataSourceID


object QuerySpec {

  def createDataSources(sc: SparkContext): Set[DataSourceID] = {
    Set(
      sc.createLocalDataSource(clusterLayoutRawData, clusterLayoutColumns, clusterLayoutMeta),
      sc.createLocalDataSource(nodeDataRawData, nodeDataColumns, nodeDataMeta),
      sc.createLocalDataSource(jobQueueRawData, jobQueueColumns, jobQueueMeta)
    )
  }

  def createSingleSourceQueryMetaEntries: Set[MetaEntry] = {
    Set(
      metaEntryFromStrings("domain", "job", "job", "identifier"),
      metaEntryFromStrings("value", "duration", "time", "seconds")
    )
  }

  def createMultipleSourceQueryMetaEntries: Set[MetaEntry] = {
    Set(
      metaEntryFromStrings("domain", "rack", "rack", "identifier"),
      metaEntryFromStrings("value", "cumulative", "flops", "count")
    )
  }

  def createMultipleSourceQueryWithDerivationMetaEntries: Set[MetaEntry] = {
    Set(
      metaEntryFromStrings("domain", "job", "job", "identifier"),
      metaEntryFromStrings("value", "cumulative", "flops", "count")
    )
  }

}

class QuerySpec extends ScrubJaySpec {

  describe("Query with single datasource solution") {
    lazy val solutions = sc.runQuery(QuerySpec.createDataSources(sc), QuerySpec.createSingleSourceQueryMetaEntries)
      .toList

    it("should have a single solution") {
      assert(solutions.length == 1)
    }
    //it("should have the correct derivation chain") {
    //  println(DataSourceID.toJsonString(solutions.head))
    //  assert(true)
    //}
    it("should find the correct datasource") {
      assert(solutions.head.realize.collect.toSet == trueJobQueue)
    }
  }

  describe("Query with multiple datasources") {

    lazy val query = new Query(QuerySpec.createDataSources(sc), QuerySpec.createMultipleSourceQueryMetaEntries)
    lazy val solutions = query.run.toList

    it("should have a single solution") {
      assert(solutions.length == 1)
    }
    //it("should have the correct derivation chain") {
    //  println(DataSourceID.toJsonString(solutions.head))
    //  assert(true)
    //}
    it("should derive the correct datasource") {
      assert(solutions.head.realize.collect.toSet == trueNodeDataJoinedWithClusterLayout)
    }
    //it("should pickle and unpickle correctly") {
    //  val unpickled = DataSourceID.fromJsonString(DataSourceID.toJsonString(solutions.head))
    //  assert(unpickled.realize.collect.toSet == trueNodeDataJoinedWithClusterLayout)
    //}
  }

  describe("Query with multiple datasources and single derivations") {
    lazy val query = new Query(QuerySpec.createDataSources(sc), QuerySpec.createMultipleSourceQueryWithDerivationMetaEntries)
    lazy val solutions = query.run.toList

    it("should have a single solution") {
      assert(solutions.length == 1)
    }
    it("should derive the correct datasource") {
      assert(solutions.head.realize.collect.toSet == trueJobQueueSpanExplodedJoinedFlops)
    }
  }
}
