package testsuite

import scrubjay.query._
import scrubjay.query.schema.{ScrubJayColumnSchemaQuery, ScrubJayDimensionSchemaQuery, ScrubJaySchemaQuery, ScrubJayUnitsSchemaQuery}
class QueryParserSpec extends ScrubJaySpec {
    describe("Testing second version of the Parser.") {
      //Default unit use unknown or any?
      it("Test D1: Normal Query. Should pass") {
        val actual = ScrubJaySchemaQuery(Set(
          ScrubJayColumnSchemaQuery(domain = Some(true), dimension = Some(ScrubJayDimensionSchemaQuery(name = Some("job")))),
          ScrubJayColumnSchemaQuery(domain = Some(false), dimension = Some(ScrubJayDimensionSchemaQuery(name = Some("time"))))
        ))
        verifyParse("SELECT DOMAIN(DIM(NAME(job))), VALUE(DIM(NAME(time)))", actual)

      }

      //They are equal when I initialize in the opposite order
      it("Test D2: Query with different ordering of Domains and Values. Should Pass") {
        val actual = ScrubJaySchemaQuery(Set(

          ScrubJayColumnSchemaQuery(domain = Some(false), dimension = Some(ScrubJayDimensionSchemaQuery(name = Some("time")))),
          ScrubJayColumnSchemaQuery(domain = Some(true), dimension = Some(ScrubJayDimensionSchemaQuery(name = Some("job"))))
        ))
        verifyParse("SELECT VALUE(DIM(NAME(time))), DOMAIN(DIM(NAME(job)))", actual)
      }
      /*
      If optional section of query is invalid, causes parse to fail and give incorrect error message.
      E.g. UNITS is optional and errors parsing it will cause the VALUE field to error out without the "Invalid Query." message.
           It will instead give the following error: "Must have at least one domain and one value"
      */

      it("Test D3: Query with non-default units. Should pass") {
        val actual = ScrubJaySchemaQuery(Set(
          ScrubJayColumnSchemaQuery(domain = Some(true), dimension = Some(ScrubJayDimensionSchemaQuery(name = Some("job")))),
          ScrubJayColumnSchemaQuery(domain = Some(false), dimension = Some(ScrubJayDimensionSchemaQuery(name = Some("time"))), units = Some(ScrubJayUnitsSchemaQuery(Some("seconds"), Some("POINT"))))
        ))

        verifyParse("SELECT DOMAIN(DIM(NAME(job))), VALUE(DIM(NAME(time)), UNITS(name(seconds), elementType(POINT)))", actual)
      }

      //Parentheses are messy.
      it("Test D4: Query with subunits. Should pass") {
        val actual = ScrubJaySchemaQuery(Set(
          ScrubJayColumnSchemaQuery(domain = Some(true), dimension = Some(ScrubJayDimensionSchemaQuery(name = Some("job")))),
          ScrubJayColumnSchemaQuery(domain = Some(false), dimension = Some(ScrubJayDimensionSchemaQuery(name = Some("time"))),
            units = Some(ScrubJayUnitsSchemaQuery(Some("seconds"), Some("POINT"),
              subUnits = Some(Map("test" -> ScrubJayUnitsSchemaQuery(Some("testName"), Some("testElem"))))))
        )))

        verifyParse("SELECT DOMAIN(DIM(NAME(job))), VALUE(DIM(NAME(time)), UNITS(name(seconds), elementType(POINT), subUnits(test:UNITS(name(testName), elementType(testElem)))))", actual)
      }

      it("Test D5: Query with multiple domains and values/testing case-insensitivity. Should pass") {
        val actual = ScrubJaySchemaQuery(Set(
          ScrubJayColumnSchemaQuery(domain = Some(true), dimension = Some(ScrubJayDimensionSchemaQuery(name = Some("job")))),
          ScrubJayColumnSchemaQuery(domain = Some(true), dimension = Some(ScrubJayDimensionSchemaQuery(name = Some("testDomain")))),
          ScrubJayColumnSchemaQuery(domain = Some(false), dimension = Some(ScrubJayDimensionSchemaQuery(name = Some("time")))),
          ScrubJayColumnSchemaQuery(domain = Some(false), dimension = Some(ScrubJayDimensionSchemaQuery(name = Some("testValue"))))
          ))

        verifyParse("SeLECT DOMAIN(DIM(NAME(job))), DoMaIN(dIm(NaMe(testDomain))), VALUE(DIM(NAME(time))), vALue(Dim(nAmE(testValue)))", actual)
      }

      it("Test E1: Missing domain. Should fail") {
        val thrown = intercept[Exception] {
          val actual = ScrubJaySchemaQuery(Set(
            ScrubJayColumnSchemaQuery(domain = Some(true), dimension = Some(ScrubJayDimensionSchemaQuery(name = Some("job")))),
            ScrubJayColumnSchemaQuery(domain = Some(false), dimension = Some(ScrubJayDimensionSchemaQuery(name = Some("time"))))
          ))
          verifyParse("SELECT VALUE(DIM(NAME(time)))", actual)
        }
        assert(thrown.getMessage.equals("Must have at least one domain and one value"))
      }
      it("Test E2: Missing value. Should fail") {
        val thrown = intercept[Exception] {
          val actual = ScrubJaySchemaQuery(Set(
            ScrubJayColumnSchemaQuery(domain = Some(true), dimension = Some(ScrubJayDimensionSchemaQuery(name = Some("job")))),
            ScrubJayColumnSchemaQuery(domain = Some(false), dimension = Some(ScrubJayDimensionSchemaQuery(name = Some("time"))))
          ))
          verifyParse("SELECT DOMAIN(DIM(NAME(job)))", actual)
        }

        assert(thrown.getMessage.equals("Must have at least one domain and one value"))
      }
      it("Test E3: Dim in wrong order. Should fail") {
        val thrown = intercept[Exception] {
          val actual = ScrubJaySchemaQuery(Set(
            ScrubJayColumnSchemaQuery(domain = Some(true), dimension = Some(ScrubJayDimensionSchemaQuery(name = Some("job")))),
            ScrubJayColumnSchemaQuery(domain = Some(false), dimension = Some(ScrubJayDimensionSchemaQuery(name = Some("time"))))
          ))
          verifyParse("SELECT DOMAIN(UNITS(foo), DIM(NAME(job))) VALUE(DIM(NAME(time)))", actual)
        }
        assert(thrown.getMessage.equals("Invalid Query."))
      }
      it("Test E4: UNITS has duplicate argument types. Should fail") {
        val thrown = intercept[Exception] {
          val actual = ScrubJaySchemaQuery(Set(
            ScrubJayColumnSchemaQuery(domain = Some(true), dimension = Some(ScrubJayDimensionSchemaQuery(name = Some("job")))),
            ScrubJayColumnSchemaQuery(domain = Some(false), dimension = Some(ScrubJayDimensionSchemaQuery(name = Some("time"))))
          ))
          verifyParse("SELECT DOMAIN(DIM(NAME(job)), UNITS(name(seconds), name(minutes), elementType(POINT)) VALUE(DIM(NAME(time)))", actual)
        }
        assert(thrown.getMessage.equals("UNITS field cannot have duplicate arguments"))
      }

    }
  def verifyParse(queryString: String, expected: ScrubJaySchemaQuery): Unit = {
    val actual = QueryParser.queryToSchema(queryString)
    assert(actual.equals(expected))
  }

}
