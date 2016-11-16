package testsuite

import scrubjay._
import scrubjay.units._
import org.joda.time.DateTime
import org.scalactic.source.Position



object InterpolationJoinSpec {
  val temperatureData = Seq(
    Map(
      "node" -> 1,
      "time" -> "2016-08-11T3:30:00+0000",
      "temp" -> 40.0
    ),
    Map(
      "node" -> 1,
      "time" -> "2016-08-11T3:31:00+0000",
      "temp" -> 50.0
    )
  )

  val flopsData = Seq(
    Map(
      "node" -> 1,
      "time" -> "2016-08-11T3:30:30+0000",
      "flops" -> 2000238
    )
  )

  val temperatureMeta = Map(
    "node" -> metaEntryFromStrings("node", "node", "identifier"),
    "time" -> metaEntryFromStrings("instant", "time", "datetimestamp"),
    "temp" -> metaEntryFromStrings("instant", "temperature", "degrees Celsius")
  )

  val flopsMeta = Map(
    "node" -> metaEntryFromStrings("node", "node", "identifier"),
    "time" -> metaEntryFromStrings("instant", "time", "datetimestamp"),
    "flops" -> metaEntryFromStrings("cumulative", "flops", "count")
  )

  val trueFlopsJoinTemp = Set(
    Map(
      "node" -> UnorderedDiscrete("1"),
      "time" -> DateTimeStamp(DateTime.parse("2016-08-11T3:30:30+0000")),
      "flops" -> OrderedDiscrete(2000238),
      "temp" -> DegreesCelsius(45.0)
    )
  )

  val trueTempJoinFlops = Set(
    Map(
      "node" -> UnorderedDiscrete("1"),
      "time" -> DateTimeStamp(DateTime.parse("2016-08-11T3:30:00+0000")),
      "flops" -> OrderedDiscrete(2000238),
      "temp" -> DegreesCelsius(40.0)
    ),
    Map(
      "node" -> UnorderedDiscrete("1"),
      "time" -> DateTimeStamp(DateTime.parse("2016-08-11T3:31:00+0000")),
      "flops" -> OrderedDiscrete(2000238),
      "temp" -> DegreesCelsius(50.0)
    )
  )
}

class InterpolationJoinSpec extends ScrubJaySpec {

  describe("InterpolationJoin") {
    lazy val temp = sc.createLocalDataSource(
      InterpolationJoinSpec.temperatureData,
      Seq("node", "time", "temp"),
      createLocalMetaSource(InterpolationJoinSpec.temperatureMeta))

    lazy val flops = sc.createLocalDataSource(
      InterpolationJoinSpec.flopsData,
      Seq("node", "time", "flops"),
      createLocalMetaSource(InterpolationJoinSpec.flopsMeta))

    describe("Many-to-one projection") {
      lazy val interjoined = flops.get.deriveInterpolationJoin(temp, 60000)

      it("should be defined") {
        assert(interjoined.isDefined)
      }

      it("should match ground truth") {
        assert(interjoined.get.rdd.collect.toSet == InterpolationJoinSpec.trueFlopsJoinTemp)
      }
    }

    describe("One-to-many projection") {
      lazy val interjoined = temp.get.deriveInterpolationJoin(flops, 60000)

      it("should be defined") {
        assert(interjoined.isDefined)
      }

      it("should match ground truth") {
        assert(interjoined.get.rdd.collect.toSet == InterpolationJoinSpec.trueTempJoinFlops)
      }
    }
  }

}
