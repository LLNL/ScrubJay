package testsuite

import scrubjay.datasetid.combination.InterpolationJoin
import scrubjay.datasetid.DatasetID
import org.scalactic.source.Position
import scrubjay.dataspace.{Dimension, DimensionSpace}

class InterpolationJoinSpec extends ScrubJaySpec {

  lazy val dimensionSpace: DimensionSpace =
    DimensionSpace(Array(
      Dimension("job", ordered = false, continuous = false),
      Dimension("node", ordered = false, continuous = false),
      Dimension("time", ordered = true, continuous = true))
    )

  describe("InterpolationJoin") {
    lazy val nodeFlops: DatasetID = DatasetID.fromJsonFile(nodeFlopsDatasetIDFilename)
    lazy val nodeTemp: DatasetID = DatasetID.fromJsonFile(nodeTempDatasetIDFilename)

    describe("Many-to-one projection") {
      lazy val interjoined = InterpolationJoin(nodeFlops, nodeTemp, 60)

      it("should be defined") {
        assert(interjoined.isValid(dimensionSpace))
      }
      it("should look correct...") {
        println(interjoined.scrubJaySchema(dimensionSpace))
        interjoined.realize(dimensionSpace).printSchema()
        interjoined.realize(dimensionSpace).show(false)
      }
      it("should pickle/unpickle correctly") {
        assert(DatasetID.fromJsonString(DatasetID.toJsonString(interjoined)) == interjoined)
      }
    }

    describe("One-to-many projection") {
      lazy val interjoined = InterpolationJoin(nodeTemp, nodeFlops, 60)

      it("should be defined") {
        assert(interjoined.isValid(dimensionSpace))
      }
      it("should look correct...") {
        println(interjoined.scrubJaySchema(dimensionSpace))
        interjoined.realize(dimensionSpace).printSchema()
        interjoined.realize(dimensionSpace).show(false)
      }
      it("should pickle/unpickle correctly") {
        assert(DatasetID.fromJsonString(DatasetID.toJsonString(interjoined)) == interjoined)
      }
    }
  }

}
