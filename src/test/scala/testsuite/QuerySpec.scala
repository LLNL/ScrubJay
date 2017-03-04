package testsuite

import scrubjay._
import scrubjay.datasource._
import scrubjay.metasource._
import scrubjay.query._

import org.scalactic.source.Position


class QuerySpec extends ScrubJaySpec {

  val dataSources: Set[DataSourceID] = Set(
    CSVDataSource(clusterLayoutFilename, CSVMetaSource(clusterLayoutMetaFilename)),
    CSVDataSource(nodeFlopsFilename, CSVMetaSource(nodeFlopsMetaFilename)),
    CSVDataSource(jobQueueFilename, CSVMetaSource(jobQueueMetaFilename))
  )

  val jobTimeQuery = Set(
    metaEntryFromStrings("domain", "job", "job", "identifier"),
    metaEntryFromStrings("value", "duration", "time", "seconds")
  )

  val rackFlopsQuery = Set(
    metaEntryFromStrings("domain", "rack", "rack", "identifier"),
    metaEntryFromStrings("value", "cumulative", "flops", "count")
  )

  val jobFlopsQuery = Set(
    metaEntryFromStrings("domain", "job", "job", "identifier"),
    metaEntryFromStrings("value", "cumulative", "flops", "count")
  )

  describe("Query with single datasource solution") {
    lazy val solutions = sc.runQuery(dataSources, jobTimeQuery)
      .toList

    it("should have a single solution") {
      assert(solutions.length == 1)
    }
    it("should find the correct datasource") {
      assert(solutions.head.realize.collect.toSet == trueJobQueue)
    }
    it("should pickle/unpickle correctly") {
      assert(DataSourceID.fromJsonString(DataSourceID.toJsonString(solutions.head)) == solutions.head)
    }
  }

  describe("Query with multiple datasources") {

    lazy val query = new Query(dataSources, rackFlopsQuery)
    lazy val solutions = query.run.toList

    it("should have a single solution") {
      assert(solutions.length == 1)
    }
    it("should derive the correct datasource") {
      assert(solutions.head.realize.collect.toSet == trueNodeDataJoinedWithClusterLayout)
    }
    it("should pickle/unpickle correctly") {
      assert(DataSourceID.fromJsonString(DataSourceID.toJsonString(solutions.head)) == solutions.head)
    }
  }

  describe("Query with multiple datasources and single derivations") {
    lazy val query = new Query(dataSources, jobFlopsQuery)
    lazy val solutions = query.run.toList

    it("should have a single solution") {
      assert(solutions.length == 1)
    }
    it("should derive the correct datasource") {
      assert(solutions.head.realize.collect.toSet == trueJobQueueSpanExplodedJoinedFlops)
    }
    it("should pickle/unpickle correctly") {
      assert(DataSourceID.fromJsonString(DataSourceID.toJsonString(solutions.head)) == solutions.head)
    }
  }
}
