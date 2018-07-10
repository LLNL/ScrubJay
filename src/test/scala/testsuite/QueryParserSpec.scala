package testsuite

import scrubjay.datasetid.{ScrubJayField, ScrubJaySchema, ScrubJayUnitsField}
import scrubjay.query._
class QueryParserSpec extends ScrubJaySpec {
    describe("Testing second version of the Parser.") {
      val defaultUnit = ScrubJayUnitsField.unknown

      it("Test D1: Normal Query. Should pass") {
        val actual = ScrubJaySchema(Array(
          ScrubJayField(domain = true, dimension = "job", units = defaultUnit),
          ScrubJayField(domain = false, dimension = "time", units = defaultUnit)
        ))
        verifyParse("SELECT DOMAIN(DIM(job)), VALUE(DIM(time))", actual)

      }
      it("Test D2: Query with different ordering of Domains and Values. Should Pass") {
        val actual = ScrubJaySchema(Array(
          ScrubJayField(domain = true, dimension = "job", units = defaultUnit),
          ScrubJayField(domain = false, dimension = "time", units = defaultUnit)
        ))
        verifyParse("SELECT VALUE(DIM(time)), DOMAIN(DIM(job))", actual)
      }
      it("Test D3: Query with non-default units. Should pass") {
        val actual = ScrubJaySchema(Array(
          ScrubJayField(domain = true, dimension = "job", units = defaultUnit),
          ScrubJayField(domain = false, dimension = "time", units = ScrubJayUnitsField("seconds", "POINT", "*", "*", Map.empty))
        ))

        verifyParse("SELECT DOMAIN(DIM(job)), VALUE(DIM(time), UNITS(name(seconds), elementType(POINT))", actual)
      }
      it("Test E1: Missing domain. Should fail") {
        intercept[Exception] {
          val actual = ScrubJaySchema(Array(
            ScrubJayField(domain = true, dimension = "job", units = defaultUnit),
            ScrubJayField(domain = false, dimension = "time", units = defaultUnit)
          ))
          verifyParse("SELECT VALUE(DIM(time))", actual)
        }
      }
      it("Test E2: Missing value. Should fail") {
        intercept[Exception] {
          val actual = ScrubJaySchema(Array(
            ScrubJayField(domain = true, dimension = "job", units = defaultUnit),
            ScrubJayField(domain = false, dimension = "time", units = defaultUnit)
          ))
          verifyParse("SELECT DOMAIN(DIM(job))", actual)
        }
      }
      it("Test E3: Dim in wrong order. Should fail") {
        intercept[Exception] {
          val actual = ScrubJaySchema(Array(
            ScrubJayField(domain = true, dimension = "job", units = defaultUnit),
            ScrubJayField(domain = false, dimension = "time", units = defaultUnit)
          ))
          verifyParse("SELECT DOMAIN(UNIT(foo), DIM(job)) VALUE(DIM(time))", actual)
        }
      }

    }
  def verifyParse(queryString: String, expected: ScrubJaySchema): Unit = {
    val qp = new QueryParser
    val actual = qp.queryToSchema(queryString)
    assert(actual.equals(expected))
  }

}
