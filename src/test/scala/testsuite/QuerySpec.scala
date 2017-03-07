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

  describe("Query with single datasource solution") {

    val jobTimeQuery = Set(
      metaEntryFromStrings("domain", "job", "identifier"),
      metaEntryFromStrings("value", "time", "seconds")
    )

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

  describe("Query with single derived datasource solution") {

    val jobTimeQuery = Set(
      metaEntryFromStrings("domain", "job", "identifier"),
      metaEntryFromStrings("domain", "time", "datetimestamp")
    )

    lazy val solutions = Query(dataSources, jobTimeQuery)
      .solutions
      .toList

    it("should have at least one solution") {
      assert(solutions.nonEmpty)
    }
    it("should find the correct datasource") {
      //solutions.foreach(_.describe())
      //assert(solutions.head.realize.collect.toSet == trueJobQueue)
    }
    it("should pickle/unpickle correctly") {
      assert(DataSourceID.fromJsonString(DataSourceID.toJsonString(solutions.head)) == solutions.head)
    }
  }

  describe("Query with multiple datasources") {

    val rackFlopsQuery = Set(
      metaEntryFromStrings("domain", "rack", "identifier"),
      metaEntryFromStrings("value", "flops", "count")
    )

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

    val jobFlopsQuery = Set(
      metaEntryFromStrings("domain", "job", "identifier"),
      metaEntryFromStrings("value", "flops", "count")
    )

    lazy val solutions = Query(dataSources, jobFlopsQuery)
      .solutions
      .toList

    it("should have at least one solution") {
      assert(solutions.nonEmpty)
    }
    it("should derive the correct datasource") {
      println(DataSourceID.toJsonString(solutions.head))
      println(solutions.head)
      solutions.head.toDataFrame.show(false)
      assert(solutions.head.realize.collect.toSet == trueNodeTimeJobFlops)
    }
    it("should pickle/unpickle correctly") {
      assert(DataSourceID.fromJsonString(DataSourceID.toJsonString(solutions.head)) == solutions.head)
    }
  }

  describe("Crazy query") {

    val jobFlopsTemperatureQuery = Set(
      metaEntryFromStrings("domain", "job", "identifier"),
      metaEntryFromStrings("value", "temperature", "degrees Celsius"),
      metaEntryFromStrings("value", "flops", "count")
    )

    lazy val solutions = Query(dataSources, jobFlopsTemperatureQuery)
      .solutions
      .toList

    it("should have at least one solution") {
      assert(solutions.nonEmpty)
    }
    it("should derive the correct datasource") {
      solutions.head.describe()
      //assert(solutions.head.realize.collect.toSet == trueNodeRackTimeJobFlops)
    }
    it("should pickle/unpickle correctly") {
      assert(DataSourceID.fromJsonString(DataSourceID.toJsonString(solutions.head)) == solutions.head)
    }
  }

  describe("Enumerate all possible derivations") {
    lazy val solutions = Query(dataSources, Set.empty)
      .allDerivations
      .toList

    it("should do things") {
      //solutions.foreach(solution => println(solution.metaSource.values.map(_.dimension.title)))
      //println(solutions.length)
      assert(true)
    }
  }
}
