package testsuite

import scrubjay.datasetid.DatasetID
import scrubjay.datasetid.original.CSVDatasetID
import scrubjay.datasetid.transformation.DeriveRate
import scrubjay.query.schema.ScrubJayDimensionSchemaQuery

class TransformationSearchSpec extends ScrubJaySpec {

  lazy val nodeFlops: DatasetID = DatasetID.fromJsonFile(nodeFlopsDatasetIDFilename)

  val rateQuery = ScrubJayDimensionSchemaQuery(
    name=Some("rate"),
    subDimensions=Some(Seq(
      ScrubJayDimensionSchemaQuery(name=Some("flops")),
      ScrubJayDimensionSchemaQuery(name=Some("time")))))

  describe("Single transformation path for single rate query") {

    val transformations = rateQuery.transformations.toSeq

    it("should have 2 solutions") {
      assert(transformations.length == 2)
    }
    it("should include no-op transformation") {
      val correctTransformation = transformations(0).apply(nodeFlops) match {
        case _: CSVDatasetID => true
        case _ => false
      }
      assert(correctTransformation)
    }

    it("should include single DeriveRate transformation") {
      val correctTransformation = transformations(1).apply(nodeFlops) match {
        case r: DeriveRate => r.dsID match {
          case _: CSVDatasetID => true
          case _ => false
        }
        case _ => false
      }
      assert(correctTransformation)
    }
  }
}
