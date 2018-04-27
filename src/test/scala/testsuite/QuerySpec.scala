package testsuite

import scrubjay.datasetid.{ScrubJayField, ScrubJaySchema, ScrubJayUnitsField}
import scrubjay.query._
import scrubjay.dataspace.DataSpace


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

      println("Query:")
      println(queryTarget)

      solutions.zipWithIndex.foreach(solution => {
        println("Solution: " + solution._2)
        solution._1.debugPrint(dataSpace.dimensionSpace)
      })
      assert(solutions.nonEmpty)
    }
  }


  describe("Query with single derived datasource solution") {

    val queryTarget = ScrubJaySchema(Array(
      ScrubJayField(domain = true, dimension = "job"),
      ScrubJayField(domain = true, dimension = "node", units = ScrubJayUnitsField("identifier", "*", "*", "*", Map.empty))
    ))

    val query = Query(dataSpace, queryTarget)

    lazy val solutions = query.solutions.toList

    it("should find the correct solution") {

      println("Query:")
      println(queryTarget)

      solutions.zipWithIndex.foreach(solution => {
        println("Solution: " + solution._2)
        solution._1.debugPrint(dataSpace.dimensionSpace)
      })
      assert(solutions.nonEmpty)
    }
  }

  describe("Query with multiple datasources") {

    val queryTarget = ScrubJaySchema(Array(
      ScrubJayField(domain = true, dimension = "rack"),
      ScrubJayField(domain = false, dimension = "flops")
    ))

    val query = Query(dataSpace, queryTarget)

    lazy val solutions = query.solutions.toList

    it("should find the correct solution") {

      println("Query:")
      println(queryTarget)

      solutions.zipWithIndex.foreach(solution => {
        println("Solution: " + solution._2)
        solution._1.debugPrint(dataSpace.dimensionSpace)
      })
      assert(solutions.nonEmpty)
    }
  }

  describe("Query with multiple datasources and single derivations") {

    val queryTarget = ScrubJaySchema(Array(
      ScrubJayField(domain = true, dimension = "job"),
      ScrubJayField(domain = true, dimension = "rack"),
      ScrubJayField(domain = false, dimension = "flops")
    ))

    val query = Query(dataSpace, queryTarget)

    lazy val solutions = query.solutions.toList

    it("should find the correct solution") {

      println("Query:")
      println(queryTarget)

      solutions.zipWithIndex.foreach(solution => {
        println("Solution: " + solution._2)
        solution._1.debugPrint(dataSpace.dimensionSpace)
      })
      assert(solutions.nonEmpty)
    }

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
