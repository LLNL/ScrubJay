package testsuite

import scrubjay.metasource._
import scrubjay.datasource._
import org.scalactic.source.Position
import scrubjay.combination.InterpolationJoin


class InterpolationJoinSpec extends ScrubJaySpec {

  describe("InterpolationJoin") {
    lazy val temp = CSVDataSource(temperatureFilename, CSVMetaSource(temperatureMetaFilename))
    lazy val flops = CSVDataSource(flopsFilename, CSVMetaSource(flopsMetaFilename))

    describe("Many-to-one projection") {
      lazy val interjoined = InterpolationJoin(flops, temp, 60000)

      it("should be defined") {
        assert(interjoined.isValid)
      }
      it("should match ground truth") {
        assert(interjoined.realize.collect.toSet == trueFlopsJoinTemp)
      }
      it("should pickle/unpickle correctly") {
        assert(DataSourceID.fromJsonString(DataSourceID.toJsonString(interjoined)) == interjoined)
      }
    }

    describe("One-to-many projection") {
      lazy val interjoined = InterpolationJoin(temp, flops, 60000)

      it("should be defined") {
        assert(interjoined.isValid)
      }
      it("should match ground truth") {
        assert(interjoined.realize.collect.toSet == trueTempJoinFlops)
      }
      it("should pickle/unpickle correctly") {
        assert(DataSourceID.fromJsonString(DataSourceID.toJsonString(interjoined)) == interjoined)
      }
    }
  }

}
