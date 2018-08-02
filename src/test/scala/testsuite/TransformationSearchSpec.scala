package testsuite

import scrubjay.datasetid.DatasetID
import scrubjay.datasetid.original.CSVDatasetID
import scrubjay.datasetid.transformation.DeriveRate
import scrubjay.query.schema.ScrubJayDimensionSchemaQuery

class TransformationSearchSpec extends ScrubJaySpec {

  lazy val nodeFlops: DatasetID = DatasetID.fromJsonFile(nodeFlopsDatasetIDFilename)

  describe("Single transformation path for single rate query") {

    val rateQuery = ScrubJayDimensionSchemaQuery(
      name=Some("rate"),
      subDimensions=Some(Seq(
        ScrubJayDimensionSchemaQuery(name=Some("flops")),
        ScrubJayDimensionSchemaQuery(name=Some("time")))))

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

  describe("Double transformation path for double rate query") {

    val rateQuery = ScrubJayDimensionSchemaQuery(
      name=Some("rate"),
      subDimensions=Some(Seq(
        ScrubJayDimensionSchemaQuery(
          name=Some("rate"),
          subDimensions=Some(Seq(
            ScrubJayDimensionSchemaQuery(name=Some("x")),
            ScrubJayDimensionSchemaQuery(name=Some("y"))
          ))),
        ScrubJayDimensionSchemaQuery(name=Some("z")))))

    val transformations = rateQuery.transformations.toSeq

    it("should have 3 solutions") {
      assert(transformations.length == 3)
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

    it("should include double DeriveRate transformation") {
      val correctTransformation = transformations(2).apply(nodeFlops) match {
        case r1: DeriveRate => r1.dsID match {
          case r2: DeriveRate => r2.dsID match {
            case _: CSVDatasetID => true
            case _ => false
          }
        }
        case _ => false
      }
      assert(correctTransformation)
    }
  }

  describe("Multiple Double transformation path for multiple double rate query") {

    val rateQuery = ScrubJayDimensionSchemaQuery(
      name=Some("rate"),
      subDimensions=Some(Seq(
        ScrubJayDimensionSchemaQuery(
          name=Some("rate"),
          subDimensions=Some(Seq(
            ScrubJayDimensionSchemaQuery(name=Some("y")),
            ScrubJayDimensionSchemaQuery(name=Some("x"))
          ))),
        ScrubJayDimensionSchemaQuery(
          name=Some("rate"),
          subDimensions=Some(Seq(
            ScrubJayDimensionSchemaQuery(name=Some("w")),
            ScrubJayDimensionSchemaQuery(name=Some("z"))
          )))
      )))

    val transformations = rateQuery.transformations.toSeq

    it("should have 5 solutions") {
      assert(transformations.length == 5)
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

    it("should include double DeriveRate transformation one way") {
      val correctTransformation = transformations(2).apply(nodeFlops) match {
        case r1: DeriveRate => r1.dsID match {
          case r2: DeriveRate
            if r2.xDimension == "z" && r2.yDimension == "w" =>
            r2.dsID match {
              case _: CSVDatasetID => true
              case _ => false
            }
        }
        case _ => false
      }
      assert(correctTransformation)
    }

    it("should include double DeriveRate transformation another way") {
      val correctTransformation = transformations(3).apply(nodeFlops) match {
        case r1: DeriveRate => r1.dsID match {
          case r2: DeriveRate
            if r2.xDimension == "x" && r2.yDimension == "y"  =>
            r2.dsID match {
              case _: CSVDatasetID => true
              case _ => false
            }
        }
        case _ => false
      }
      assert(correctTransformation)
    }

    it("should include double double DeriveRate transformation (animal style)") {
      val correctTransformation = transformations(4).apply(nodeFlops) match {
        case r1: DeriveRate => r1.dsID match {
          case r2: DeriveRate
            if r2.xDimension == "x" && r2.yDimension == "y"  =>
            r2.dsID match {
              case r3: DeriveRate
                if r3.xDimension == "z" && r3.yDimension == "w" =>
                r3.dsID match {
                  case _: CSVDatasetID => true
                  case _ => false
                }
            }
        }
        case _ => false
      }
      assert(correctTransformation)
    }
  }
}
