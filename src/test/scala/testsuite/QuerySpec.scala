package testsuite

import scrubjay.query._
import scrubjay.dataspace.DataSpace
import scrubjay.query.schema.{ScrubJayColumnSchemaQuery, ScrubJaySchemaQuery, ScrubJayUnitsQuery}


class QuerySpec extends ScrubJaySpec {

  val dataSpace: DataSpace = DataSpace.fromJsonFile(jobAnalysisDataSpaceFilename)

  describe("Query with NO solution") {

    val queryTarget = ScrubJaySchemaQuery(Set(
      ScrubJayColumnSchemaQuery(domain = Some(true), dimension = Some("job")),
      ScrubJayColumnSchemaQuery(domain = Some(false), dimension = Some("marmosets"))
    ))

    val query = Query(dataSpace, queryTarget)

    lazy val solutions = query.solutions.toList
    it("should find no correct solution") {
      assert(solutions.isEmpty)
    }
  }

  describe("Query with single original dataset solution") {

    val queryTarget = ScrubJaySchemaQuery(Set(
      ScrubJayColumnSchemaQuery(domain = Some(true), dimension = Some("job")),
      ScrubJayColumnSchemaQuery(domain = Some(false), dimension = Some("time"))
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

    val queryTarget = ScrubJaySchemaQuery(Set(
      ScrubJayColumnSchemaQuery(domain = Some(true), dimension = Some("job")),
      ScrubJayColumnSchemaQuery(domain = Some(true), dimension = Some("node"), units = Some(ScrubJayUnitsQuery(Some("identifier"))))
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

    val queryTarget = ScrubJaySchemaQuery(Set(
      ScrubJayColumnSchemaQuery(domain = Some(true), dimension = Some("rack")),
      ScrubJayColumnSchemaQuery(domain = Some(false), dimension = Some("flops"))
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

    val queryTarget = ScrubJaySchemaQuery(Set(
      ScrubJayColumnSchemaQuery(domain = Some(true), dimension = Some("job")),
      ScrubJayColumnSchemaQuery(domain = Some(true), dimension = Some("rack")),
      ScrubJayColumnSchemaQuery(domain = Some(false), dimension = Some("flops"))
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
