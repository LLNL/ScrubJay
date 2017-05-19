package testsuite

import scrubjay.datasetid.{ScrubJayField, ScrubJaySchema}
import scrubjay.query._
import scrubjay.dataspace.{DataSpace, DimensionSpace}


class QuerySpec extends ScrubJaySpec {

  val dataSpace: DataSpace = DataSpace.fromJsonFile(jobAnalysisDataSpaceFilename)

  describe("Query with NO solution") {

    val queryTarget = ScrubJaySchema(Array(
      ScrubJayField(domain = true, dimension = "job"),
      ScrubJayField(domain = false, dimension = "marmosets")
    ))

    val query = Query(dataSpace, queryTarget)

    lazy val solutions = query.solutions.toList

    it("should find no correct solution") {
      assert(solutions.isEmpty)
    }
  }

  describe("Query with single original dataset solution") {

    val queryTarget = ScrubJaySchema(Array(
      ScrubJayField(domain = true, dimension = "job"),
      ScrubJayField(domain = false, dimension = "time")
    ))

    val query = Query(dataSpace, queryTarget)

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

    val queryTarget = ScrubJaySchema(Array(
      ScrubJayField(domain = true, dimension = "job"),
      ScrubJayField(domain = true, dimension = "node", units = "identifier")
    ))

    val query = Query(dataSpace, queryTarget)

    lazy val solutions = query.solutions.toList

    it("should find the correct solution") {
      assert(solutions.nonEmpty)
    }
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
