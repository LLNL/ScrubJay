package testsuite

import scrubjay.query._
import scrubjay.dataspace.{DataSpace, DimensionSpace}


class QuerySpec extends ScrubJaySpec {

  val dataSpace: DataSpace = DataSpace.fromJsonFile(jobAnalysisDataSpaceFilename)

  describe("Query with single original dataset solution") {

    val domainDimensions = DimensionSpace(Array(
      dataSpace.dimension("job")
    ))

    val valueDimensions = DimensionSpace(Array(
      dataSpace.dimension("time")
    ))

    val query = Query(dataSpace, domainDimensions, valueDimensions)

    lazy val solutions = query.solutions.toList

    it("should find the correct solution") {
      solutions.foreach(solution => {
        println("Solution: ")
        solution.realize(dataSpace.dimensionSpace).show(false)
      })
      assert(solutions.nonEmpty)
    }
  }

  describe("Query with single derived datasource solution") {

    /*
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
      //solutions.head.describe()
      assert(solutions.head.realize.collect.toSet == trueJobQueueExplodedTime)
    }
    it("should pickle/unpickle correctly") {
      assert(DatasetID.fromJsonString(DatasetID.toJsonString(solutions.head)) == solutions.head)
    }
    */
  }

  describe("Query with multiple datasources") {

    /*
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
      //solutions.head.describe()
      assert(solutions.head.realize.collect.toSet == trueNodeRackTimeFlops)
    }
    it("should pickle/unpickle correctly") {
      assert(DatasetID.fromJsonString(DatasetID.toJsonString(solutions.head)) == solutions.head)
    }
    */
  }

  describe("Query with multiple datasources and single derivations") {

    /*
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
      //solutions.head.describe()
      //println(DatasetID.toDotString(solutions.head))
      assert(solutions.head.realize.collect.toSet == trueNodeTimeJobFlops)
    }
    it("should pickle/unpickle correctly") {
      assert(DatasetID.fromJsonString(DatasetID.toJsonString(solutions.head)) == solutions.head)
    }
    */
  }

  describe("Enumerate all possible derivations") {
    /*
    lazy val solutions = Query(dataSources, Set.empty)
      .allDerivations

    it("should do things") {
      solutions.foreach(solution => {
        println(DatasetID.toDotString(solution))
      })
      assert(true)
    }
    */
  }
}
