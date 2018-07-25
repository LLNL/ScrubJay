package testsuite

import scrubjay.datasetid.combination.InterpolationJoin
import scrubjay.datasetid.DatasetID
import scrubjay.dataspace.DimensionSpace
import scrubjay.schema.ScrubJayDimensionSchema

class InterpolationJoinSpec extends ScrubJaySpec {

  describe("InterpolationJoin") {
    lazy val nodeFlops: DatasetID = DatasetID.fromJsonFile(nodeFlopsDatasetIDFilename)
    lazy val nodeTemp: DatasetID = DatasetID.fromJsonFile(nodeTempDatasetIDFilename)

    describe("Many-to-one projection") {
      lazy val interjoined = InterpolationJoin(nodeFlops, nodeTemp, 60)

      it("should be defined") {
        assert(interjoined.isValid)
      }
      it("should look correct"){
        interjoined.debugPrint
      }
      it("should serialize/deserialize correctly") {
        assert(DatasetID.fromJsonString(DatasetID.toJsonString(interjoined)) == interjoined)
      }
    }

    describe("One-to-many projection") {
      lazy val interjoined = InterpolationJoin(nodeTemp, nodeFlops, 60)

      it("should be defined") {
        assert(interjoined.isValid)
      }
      it("should look correct") {
        interjoined.debugPrint
      }
      it("should serialize/deserialize correctly") {
        assert(DatasetID.fromJsonString(DatasetID.toJsonString(interjoined)) == interjoined)
      }
    }
  }

}
