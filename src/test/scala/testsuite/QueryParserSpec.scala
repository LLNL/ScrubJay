package testsuite

import scrubjay.query._
import scrubjay.query.schema.{ScrubJayColumnSchemaQuery, ScrubJaySchemaQuery, ScrubJayUnitsQuery}
class QueryParserSpec extends ScrubJaySpec {
    describe("Testing second version of the Parser.") {
      //Default unit use unknown or any?
      it("Test D1: Normal Query. Should pass") {
        val actual = ScrubJaySchemaQuery(Set(
          ScrubJayColumnSchemaQuery(domain = Some(true), dimension = Some("job")),
          ScrubJayColumnSchemaQuery(domain = Some(false), dimension = Some("time"))
        ))
        verifyParse("SELECT DOMAIN(DIM(job)), VALUE(DIM(time))", actual)

      }

      //They are equal when I initialize in the opposite order
      it("Test D2: Query with different ordering of Domains and Values. Should Pass") {
        val actual = ScrubJaySchemaQuery(Set(

          ScrubJayColumnSchemaQuery(domain = Some(false), dimension = Some("time")),
          ScrubJayColumnSchemaQuery(domain = Some(true), dimension = Some("job"))
        ))
        verifyParse("SELECT VALUE(DIM(time)), DOMAIN(DIM(job))", actual)
      }
      /*
      If optional section of query is invalid, causes parse to fail and give incorrect error message.
      E.g. UNITS is optional and errors parsing it will cause the VALUE field to error out without the "Invalid Query." message.
           It will instead give the following error: "Must have at least one domain and one value"
      */

      it("Test D3: Query with non-default units. Should pass") {
        val actual = ScrubJaySchemaQuery(Set(
          ScrubJayColumnSchemaQuery(domain = Some(true), dimension = Some("job")),
          ScrubJayColumnSchemaQuery(domain = Some(false), dimension = Some("time"), units = Some(ScrubJayUnitsQuery(Some("seconds"), Some("POINT"))))
        ))

        verifyParse("SELECT DOMAIN(DIM(job)), VALUE(DIM(time), UNITS(name(seconds), elementType(POINT)))", actual)
      }

      //Parentheses are messy.
      it("Test D4: Query with subunits. Should pass") {
        val actual = ScrubJaySchemaQuery(Set(
          ScrubJayColumnSchemaQuery(domain = Some(true), dimension = Some("job")),
          ScrubJayColumnSchemaQuery(domain = Some(false), dimension = Some("time"),
            units = Some(ScrubJayUnitsQuery(Some("seconds"), Some("POINT"),
              subUnits = Some(Map("test" -> ScrubJayUnitsQuery(Some("testName"), Some("testElem"))))))
        )))

        verifyParse("SELECT DOMAIN(DIM(job)), VALUE(DIM(time), UNITS(name(seconds), elementType(POINT), subUnits(test:UNITS(name(testName), elementType(testElem)))))", actual)
      }

      it("Test D5: Query with multiple domains and values/testing case-insensitivity. Should pass") {
        val actual = ScrubJaySchemaQuery(Set(
          ScrubJayColumnSchemaQuery(domain = Some(true), dimension = Some("job")),
          ScrubJayColumnSchemaQuery(domain = Some(true), dimension = Some("testDomain")),
          ScrubJayColumnSchemaQuery(domain = Some(false), dimension = Some("time")),
          ScrubJayColumnSchemaQuery(domain = Some(false), dimension = Some("testValue"))
          ))

        verifyParse("SeLECT DOMAIN(DIM(job)), DoMaIN(dIm(testDomain)), VALUE(DIM(time)), vALue(Dim(testValue))", actual)
      }

      it("Test E1: Missing domain. Should fail") {
        val thrown = intercept[Exception] {
          val actual = ScrubJaySchemaQuery(Set(
            ScrubJayColumnSchemaQuery(domain = Some(true), dimension = Some("job")),
            ScrubJayColumnSchemaQuery(domain = Some(false), dimension = Some("time"))
          ))
          verifyParse("SELECT VALUE(DIM(time))", actual)
        }
        assert(thrown.getMessage.equals("Must have at least one domain and one value"))
      }
      it("Test E2: Missing value. Should fail") {
        val thrown = intercept[Exception] {
          val actual = ScrubJaySchemaQuery(Set(
            ScrubJayColumnSchemaQuery(domain = Some(true), dimension = Some("job")),
            ScrubJayColumnSchemaQuery(domain = Some(false), dimension = Some("time"))
          ))
          verifyParse("SELECT DOMAIN(DIM(job))", actual)
        }

        assert(thrown.getMessage.equals("Must have at least one domain and one value"))
      }
      it("Test E3: Dim in wrong order. Should fail") {
        val thrown = intercept[Exception] {
          val actual = ScrubJaySchemaQuery(Set(
            ScrubJayColumnSchemaQuery(domain = Some(true), dimension = Some("job")),
            ScrubJayColumnSchemaQuery(domain = Some(false), dimension = Some("time"))
          ))
          verifyParse("SELECT DOMAIN(UNITS(foo), DIM(job)) VALUE(DIM(time))", actual)
        }
        assert(thrown.getMessage.equals("Invalid Query."))
      }
      it("Test E4: UNITS has duplicate argument types. Should fail") {
        val thrown = intercept[Exception] {
          val actual = ScrubJaySchemaQuery(Set(
            ScrubJayColumnSchemaQuery(domain = Some(true), dimension = Some("job")),
            ScrubJayColumnSchemaQuery(domain = Some(false), dimension = Some("time"))
          ))
          verifyParse("SELECT DOMAIN(DIM(job), UNITS(name(seconds), name(minutes), elementType(POINT)) VALUE(DIM(time))", actual)
        }
        assert(thrown.getMessage.equals("UNITS field cannot have duplicate arguments"))
      }

    }
  def verifyParse(queryString: String, expected: ScrubJaySchemaQuery): Unit = {
    val qp = new QueryParser
    val actual = qp.queryToSchema(queryString)
    assert(actual.equals(expected))
  }

}
