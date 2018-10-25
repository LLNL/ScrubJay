// Copyright 2018 Lawrence Livermore National Security, LLC and other
// ScrubJay Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: (Apache-2.0 OR MIT)

package testsuite

import scrubjay.query._
import scrubjay.dataspace.DataSpace
import scrubjay.query.schema.{ScrubJayColumnSchemaQuery, ScrubJayDimensionSchemaQuery, ScrubJaySchemaQuery, ScrubJayUnitsSchemaQuery}


class QuerySpec extends ScrubJaySpec {

  val dataSpace: DataSpace = DataSpace.fromJsonFile(jobAnalysisDataSpaceFilename)

  describe("Query with NO solution") {

    val queryTarget = ScrubJaySchemaQuery(Set(
      ScrubJayColumnSchemaQuery(domain = Some(true), dimension = Some(ScrubJayDimensionSchemaQuery(name = Some("job")))),
      ScrubJayColumnSchemaQuery(domain = Some(false), dimension = Some(ScrubJayDimensionSchemaQuery(name = Some("marmosets"))))
    ))

    val query = Query(dataSpace, queryTarget)

    it("should find no correct solution") {

      println("Query:")
      println(queryTarget)

      val solutions = query.solutions.toList

      assert(solutions.isEmpty)
    }
  }

  describe("Query with single original dataset solution") {

    val queryTarget = ScrubJaySchemaQuery(Set(
      ScrubJayColumnSchemaQuery(domain = Some(true), dimension = Some(ScrubJayDimensionSchemaQuery(name = Some("job")))),
      ScrubJayColumnSchemaQuery(domain = Some(false), dimension = Some(ScrubJayDimensionSchemaQuery(name = Some("time"))))
    ))

    val query = Query(dataSpace, queryTarget)

    it("should find the correct solution") {

      println("Query:")
      println(queryTarget)

      val solutions = query.solutions.toList

      solutions.zipWithIndex.foreach(solution => {
        println("Solution: " + solution._2)
        solution._1.debugPrint
      })
      assert(solutions.nonEmpty)
    }
  }


  describe("Query with single decomposed datasource solution") {

    val queryTarget = ScrubJaySchemaQuery(Set(
      ScrubJayColumnSchemaQuery(domain = Some(true), dimension = Some(ScrubJayDimensionSchemaQuery(name = Some("job")))),
      ScrubJayColumnSchemaQuery(domain = Some(true), dimension = Some(ScrubJayDimensionSchemaQuery(name = Some("node"))), units = Some(ScrubJayUnitsSchemaQuery(Some("identifier"))))
    ))

    val query = Query(dataSpace, queryTarget)


    it("should find the correct solution") {

      println("Query:")
      println(queryTarget)

      val solutions = query.solutions.toList

      solutions.zipWithIndex.foreach(solution => {
        println("Solution: " + solution._2)
        solution._1.debugPrint
      })
      assert(solutions.nonEmpty)
    }
  }

  describe("Query with single derived datasource solution") {

    val queryTarget = ScrubJaySchemaQuery(Set(
      ScrubJayColumnSchemaQuery(
        domain = Some(false),
        dimension = Some(ScrubJayDimensionSchemaQuery(
          name = Some("rate"),
          subDimensions = Some(
            Seq(
              ScrubJayDimensionSchemaQuery(name=Some("flops")),
              ScrubJayDimensionSchemaQuery(name=Some("time"))
            )))
        ))
    ))

    val query = Query(dataSpace, queryTarget)


    it("should find the correct solution") {

      println("Query:")
      println(queryTarget)

      val solutions = query.solutions.toList

      solutions.zipWithIndex.foreach(solution => {
        println("Solution: " + solution._2)
        solution._1.debugPrint
      })
      assert(solutions.length > 0)
      assert(solutions.forall(solution => queryTarget.matches(solution.scrubJaySchema)))
    }
  }

  describe("Query with multiple datasources") {

    val queryTarget = ScrubJaySchemaQuery(Set(
      ScrubJayColumnSchemaQuery(domain = Some(true), dimension = Some(ScrubJayDimensionSchemaQuery(name = Some("rack")))),
      ScrubJayColumnSchemaQuery(domain = Some(false), dimension = Some(ScrubJayDimensionSchemaQuery(name = Some("flops"))))
    ))

    val query = Query(dataSpace, queryTarget)


    it("should find the correct solution") {

      println("Query:")
      println(queryTarget)

      val solutions = query.solutions.toList

      solutions.zipWithIndex.foreach(solution => {
        println("Solution: " + solution._2)
        solution._1.debugPrint
      })
      assert(solutions.nonEmpty)
    }
  }

  describe("Query with multiple datasources and single derivations") {

    val queryTarget = ScrubJaySchemaQuery(Set(
      ScrubJayColumnSchemaQuery(domain = Some(true), dimension = Some(ScrubJayDimensionSchemaQuery(name = Some("job")))),
      ScrubJayColumnSchemaQuery(domain = Some(true), dimension = Some(ScrubJayDimensionSchemaQuery(name = Some("rack")))),
      ScrubJayColumnSchemaQuery(domain = Some(false), dimension = Some(ScrubJayDimensionSchemaQuery(name = Some("flops"))))
    ))

    val query = Query(dataSpace, queryTarget)

    it("should find the correct solution") {

      println("Query:")
      println(queryTarget)

      val solutions = query.solutions.toList

      solutions.zipWithIndex.foreach(solution => {
        println("Solution: " + solution._2)
        solution._1.debugPrint
      })
      assert(solutions.nonEmpty)
    }

  }

  describe("Enumerate all possible derivations") {
    lazy val solutions = Query.allDerivations(dataSpace)

    it("should do things") {
      solutions.foreach(solution => {
        solution.debugPrint
      })
      assert(true)
    }
  }
}
