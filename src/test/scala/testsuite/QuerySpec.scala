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
    CSVDataSource(temperatureFilename, CSVMetaSource(temperatureMetaFilename)),
    CSVDataSource(jobQueueFilename, CSVMetaSource(jobQueueMetaFilename))
  )

  val jobTimeQuery = Set(
    metaEntryFromStrings("domain", "job", "identifier"),
    metaEntryFromStrings("value", "time", "seconds")
  )

  val rackFlopsQuery = Set(
    metaEntryFromStrings("domain", "rack", "identifier"),
    metaEntryFromStrings("value", "flops", "count")
  )

  val jobFlopsQuery = Set(
    metaEntryFromStrings("domain", "job", "identifier"),
    metaEntryFromStrings("value", "flops", "count")
  )

  val jobFlopsTemperatureQuery = Set(
    metaEntryFromStrings("domain", "job", "identifier"),
    metaEntryFromStrings("value", "temperature", "degrees Celsius"),
    metaEntryFromStrings("value", "flops", "count")
  )

  describe("Query with single datasource solution") {
    lazy val solutions = Query(dataSources, jobTimeQuery)
      .solutions
      .toList

    it("should have at least one solution") {
      assert(solutions.nonEmpty)
    }
    it("should find the correct datasource") {
      assert(solutions.head.realize.collect.toSet == trueJobQueue)
    }
    it("should pickle/unpickle correctly") {
      assert(DataSourceID.fromJsonString(DataSourceID.toJsonString(solutions.head)) == solutions.head)
    }
  }

  describe("Query with multiple datasources") {

    lazy val solutions = Query(dataSources, rackFlopsQuery)
      .solutions
      .toList

    it("should have at least one solution") {
      assert(solutions.nonEmpty)
    }
    it("should derive the correct datasource") {
      assert(solutions.head.realize.collect.toSet == trueNodeRackTimeFlops)
    }
    it("should pickle/unpickle correctly") {
      assert(DataSourceID.fromJsonString(DataSourceID.toJsonString(solutions.head)) == solutions.head)
    }
  }

  describe("Query with multiple datasources and single derivations") {
    lazy val solutions = Query(dataSources, jobFlopsQuery)
      .solutions
      .toList

    it("should have at least one solution") {
      assert(solutions.nonEmpty)
    }
    it("should derive the correct datasource") {
      assert(solutions.head.realize.collect.toSet == trueNodeTimeJobFlops)
    }
    it("should pickle/unpickle correctly") {
      assert(DataSourceID.fromJsonString(DataSourceID.toJsonString(solutions.head)) == solutions.head)
    }
  }

  describe("Crazy query") {
    lazy val solutions = Query(dataSources, jobFlopsTemperatureQuery)
      .solutions
      .toList

    it("should have at least one solution") {
      assert(solutions.nonEmpty)
    }
    it("should derive the correct datasource") {
      solutions.head.saveToCSV("crazy.csv")
      //assert(solutions.head.realize.collect.toSet == trueNodeRackTimeJobFlops)
    }
    it("should pickle/unpickle correctly") {
      assert(DataSourceID.fromJsonString(DataSourceID.toJsonString(solutions.head)) == solutions.head)
    }
  }

  describe("Enumerate all possible derivations") {
    lazy val solutions = Query(dataSources, jobFlopsQuery)
      .allDerivations
      .toList

    it("should do things") {
      //solutions.foreach(solution => println(solution.metaSource.values.map(_.dimension.title)))
      //println(solutions.length)
      assert(true)
    }
  }
}
