import scrubjay._
import scrubjay.units._

import org.joda.time.{DateTime, Interval}

package object testsuite {

  /*
   * Test CSV file paths from test resources directory
   */

  val temperatureMetaFilename: String = getClass.getResource("/temperatureMeta.csv").getPath
  val temperatureFilename: String = getClass.getResource("/temperature.csv").getPath

  val flopsMetaFilename: String = getClass.getResource("/flopsMeta.csv").getPath
  val flopsFilename: String = getClass.getResource("/flops.csv").getPath

  val jobQueueMetaFilename: String = getClass.getResource("/jobQueueMeta.csv").getPath
  val jobQueueFilename: String = getClass.getResource("/jobQueue.csv").getPath

  val nodeFlopsMetaFilename: String = getClass.getResource("/nodeFlopsMeta.csv").getPath
  val nodeFlopsFilename: String = getClass.getResource("/nodeFlops.csv").getPath

  val clusterLayoutMetaFilename: String = getClass.getResource("/clusterLayoutMeta.csv").getPath
  val clusterLayoutFilename: String = getClass.getResource("/clusterLayout.csv").getPath

  /*
   * Ground truth
   */

  val trueJobQueue = Set(
    Map(
      "jobid" -> UnorderedDiscrete("456"),
      "elapsed" -> Seconds(45.0),
      "end" -> DateTimeStamp(DateTime.parse("2016-08-11T03:32:00.000Z").getMillis),
      "start" -> DateTimeStamp(DateTime.parse("2016-08-11T03:30:00.000Z").getMillis),
      "nodelist" -> UnitsList(List(UnorderedDiscrete("4"), UnorderedDiscrete("5"), UnorderedDiscrete("6")))
    ),
    Map(
      "jobid" -> UnorderedDiscrete("123"),
      "elapsed" -> Seconds(23.0),
      "end" -> DateTimeStamp(DateTime.parse("2016-08-11T03:31:00.000Z").getMillis),
      "start" -> DateTimeStamp(DateTime.parse("2016-08-11T03:30:00.000Z").getMillis),
      "nodelist" -> UnitsList(List(UnorderedDiscrete("1"), UnorderedDiscrete("2"), UnorderedDiscrete("3")))
    )
  )

  val trueCabLayout = Set(
    Map("node" -> UnorderedDiscrete("1"), "rack" -> UnorderedDiscrete("1")),
    Map("node" -> UnorderedDiscrete("2"), "rack" -> UnorderedDiscrete("1")),
    Map("node" -> UnorderedDiscrete("3"), "rack" -> UnorderedDiscrete("1")),
    Map("node" -> UnorderedDiscrete("5"), "rack" -> UnorderedDiscrete("2")),
    Map("node" -> UnorderedDiscrete("6"), "rack" -> UnorderedDiscrete("2")),
    Map("node" -> UnorderedDiscrete("4"), "rack" -> UnorderedDiscrete("2"))
  )

  val trueJobQueueSpan = Set(
    Map(
     "span"-> DateTimeSpan((DateTime.parse("2016-08-11T03:30:00.000Z").getMillis, DateTime.parse("2016-08-11T03:32:00.000Z").getMillis)),
     "jobid" -> UnorderedDiscrete("456"),
     "elapsed" -> Seconds(45.0),
     "nodelist" -> UnitsList(List(UnorderedDiscrete("4"), UnorderedDiscrete("5"), UnorderedDiscrete("6")))),
    Map(
      "span" -> DateTimeSpan((DateTime.parse("2016-08-11T03:30:00.000Z").getMillis,DateTime.parse("2016-08-11T03:31:00.000Z").getMillis)),
      "jobid" -> UnorderedDiscrete("123"),
      "elapsed" -> Seconds(23.0),
      "nodelist" -> UnitsList(List(UnorderedDiscrete("1"), UnorderedDiscrete("2"), UnorderedDiscrete("3")))
    )
  )

  val trueJobQueueSpanExploded = Set(
    Map(
      "span"-> DateTimeSpan((DateTime.parse("2016-08-11T03:30:00.000Z").getMillis,DateTime.parse("2016-08-11T03:32:00.000Z").getMillis)),
      "jobid" -> UnorderedDiscrete("456"),
      "elapsed" -> Seconds(45.0),
      "nodelist_exploded" -> UnorderedDiscrete("4")),
    Map(
      "span"-> DateTimeSpan((DateTime.parse("2016-08-11T03:30:00.000Z").getMillis,DateTime.parse("2016-08-11T03:32:00.000Z").getMillis)),
      "jobid" -> UnorderedDiscrete("456"),
      "elapsed" -> Seconds(45.0),
      "nodelist_exploded" -> UnorderedDiscrete("5")),
    Map(
      "span"-> DateTimeSpan((DateTime.parse("2016-08-11T03:30:00.000Z").getMillis,DateTime.parse("2016-08-11T03:32:00.000Z").getMillis)),
      "jobid" -> UnorderedDiscrete("456"),
      "elapsed" -> Seconds(45.0),
      "nodelist_exploded" -> UnorderedDiscrete("6")),
    Map(
      "span" -> DateTimeSpan((DateTime.parse("2016-08-11T03:30:00.000Z").getMillis,DateTime.parse("2016-08-11T03:31:00.000Z").getMillis)),
      "jobid" -> UnorderedDiscrete("123"),
      "elapsed" -> Seconds(23.0),
      "nodelist_exploded" -> UnorderedDiscrete("1")),
    Map(
      "span" -> DateTimeSpan((DateTime.parse("2016-08-11T03:30:00.000Z").getMillis,DateTime.parse("2016-08-11T03:31:00.000Z").getMillis)),
      "jobid" -> UnorderedDiscrete("123"),
      "elapsed" -> Seconds(23.0),
      "nodelist_exploded" -> UnorderedDiscrete("2")),
    Map(
      "span" -> DateTimeSpan((DateTime.parse("2016-08-11T03:30:00.000Z").getMillis,DateTime.parse("2016-08-11T03:31:00.000Z").getMillis)),
      "jobid" -> UnorderedDiscrete("123"),
      "elapsed" -> Seconds(23.0),
      "nodelist_exploded" -> UnorderedDiscrete("3"))
  )

  val trueFlopsJoinTemp = Set(
    Map(
      "node" -> UnorderedDiscrete("1"),
      "time" -> DateTimeStamp(DateTime.parse("2016-08-11T3:30:30+0000").getMillis),
      "flops" -> OrderedDiscrete(2000238),
      "temp" -> DegreesCelsius(45.0)
    )
  )

  val trueTempJoinFlops = Set(
    Map(
      "node" -> UnorderedDiscrete("1"),
      "time" -> DateTimeStamp(DateTime.parse("2016-08-11T3:30:00+0000").getMillis),
      "flops" -> OrderedDiscrete(2000238),
      "temp" -> DegreesCelsius(40.0)
    ),
    Map(
      "node" -> UnorderedDiscrete("1"),
      "time" -> DateTimeStamp(DateTime.parse("2016-08-11T3:31:00+0000").getMillis),
      "flops" -> OrderedDiscrete(2000238),
      "temp" -> DegreesCelsius(50.0)
    )
  )

  val trueJobQueueSpanExplodedJoined = Set(
    Map(
      "span"-> DateTimeSpan((DateTime.parse("2016-08-11T03:30:00.000Z").getMillis,DateTime.parse("2016-08-11T03:32:00.000Z").getMillis)),
      "jobid" -> UnorderedDiscrete("456"),
      "elapsed" -> Seconds(45.0),
      "rack" -> UnorderedDiscrete("2"),
      "nodelist_exploded" -> UnorderedDiscrete("4")),
    Map(
      "span"-> DateTimeSpan((DateTime.parse("2016-08-11T03:30:00.000Z").getMillis,DateTime.parse("2016-08-11T03:32:00.000Z").getMillis)),
      "jobid" -> UnorderedDiscrete("456"),
      "elapsed" -> Seconds(45.0),
      "rack" -> UnorderedDiscrete("2"),
      "nodelist_exploded" -> UnorderedDiscrete("5")),
    Map(
      "span"-> DateTimeSpan((DateTime.parse("2016-08-11T03:30:00.000Z").getMillis,DateTime.parse("2016-08-11T03:32:00.000Z").getMillis)),
      "jobid" -> UnorderedDiscrete("456"),
      "elapsed" -> Seconds(45.0),
      "rack" -> UnorderedDiscrete("2"),
      "nodelist_exploded" -> UnorderedDiscrete("6")),
    Map(
      "span" -> DateTimeSpan((DateTime.parse("2016-08-11T03:30:00.000Z").getMillis,DateTime.parse("2016-08-11T03:31:00.000Z").getMillis)),
      "jobid" -> UnorderedDiscrete("123"),
      "elapsed" -> Seconds(23.0),
      "rack" -> UnorderedDiscrete("1"),
      "nodelist_exploded" -> UnorderedDiscrete("1")),
    Map(
      "span" -> DateTimeSpan((DateTime.parse("2016-08-11T03:30:00.000Z").getMillis,DateTime.parse("2016-08-11T03:31:00.000Z").getMillis)),
      "jobid" -> UnorderedDiscrete("123"),
      "elapsed" -> Seconds(23.0),
      "rack" -> UnorderedDiscrete("1"),
      "nodelist_exploded" -> UnorderedDiscrete("2")),
    Map(
      "span" -> DateTimeSpan((DateTime.parse("2016-08-11T03:30:00.000Z").getMillis,DateTime.parse("2016-08-11T03:31:00.000Z").getMillis)),
      "jobid" -> UnorderedDiscrete("123"),
      "elapsed" -> Seconds(23.0),
      "rack" -> UnorderedDiscrete("1"),
      "nodelist_exploded" -> UnorderedDiscrete("3"))
  )

  val trueJobQueueSpanExplodedJoinedFlops = Set(
    Map(
      "span" -> DateTimeSpan((DateTime.parse("2016-08-11T03:30:00.000Z").getMillis,DateTime.parse("2016-08-11T03:32:00.000Z").getMillis)),
      "jobid" -> UnorderedDiscrete("456"),
      "elapsed" -> Seconds(45.0),
      "flops" -> OrderedDiscrete(37614),
      "nodelist_exploded" -> UnorderedDiscrete("5"),
      "rack" -> UnorderedDiscrete("2")),
    Map(
      "span" -> DateTimeSpan((DateTime.parse("2016-08-11T03:30:00.000Z").getMillis,DateTime.parse("2016-08-11T03:32:00.000Z").getMillis)),
      "jobid" -> UnorderedDiscrete("456"),
      "elapsed" -> Seconds(45.0),
      "flops" -> OrderedDiscrete(20922),
      "nodelist_exploded" -> UnorderedDiscrete("6"),
      "rack" -> UnorderedDiscrete("2")),
    Map(
      "span" -> DateTimeSpan((DateTime.parse("2016-08-11T03:30:00.000Z").getMillis,DateTime.parse("2016-08-11T03:31:00.000Z").getMillis)),
      "jobid" -> UnorderedDiscrete("123"),
      "elapsed" -> Seconds(23.0),
      "flops" -> OrderedDiscrete(23334),
      "nodelist_exploded" -> UnorderedDiscrete("1"),
      "rack" -> UnorderedDiscrete("1")),
    Map(
      "span" -> DateTimeSpan((DateTime.parse("2016-08-11T03:30:00.000Z").getMillis,DateTime.parse("2016-08-11T03:31:00.000Z").getMillis)),
      "jobid" -> UnorderedDiscrete("123"),
      "elapsed" -> Seconds(23.0),
      "flops" -> OrderedDiscrete(1099),
      "nodelist_exploded" -> UnorderedDiscrete("3"),
      "rack" -> UnorderedDiscrete("1")),
    Map(
      "span" -> DateTimeSpan((DateTime.parse("2016-08-11T03:30:00.000Z").getMillis,DateTime.parse("2016-08-11T03:32:00.000Z").getMillis)),
      "jobid" -> UnorderedDiscrete("456"),
      "elapsed" -> Seconds(45.0),
      "flops" -> OrderedDiscrete(117478),
      "nodelist_exploded" -> UnorderedDiscrete("4"),
      "rack" -> UnorderedDiscrete("2")),
    Map(
      "span" -> DateTimeSpan((DateTime.parse("2016-08-11T03:30:00.000Z").getMillis,DateTime.parse("2016-08-11T03:31:00.000Z").getMillis)),
      "jobid" -> UnorderedDiscrete("123"),
      "elapsed" -> Seconds(23.0),
      "flops" -> OrderedDiscrete(35225),
      "nodelist_exploded" -> UnorderedDiscrete("2"),
      "rack" -> UnorderedDiscrete("1"))
  )

  val trueNodeDataJoinedWithClusterLayout = Set(
    Map("node" -> UnorderedDiscrete("4"), "rack" -> UnorderedDiscrete("2"), "time" -> DateTimeStamp(DateTime.parse("2016-08-11T03:30:00.000Z").getMillis), "flops" -> OrderedDiscrete(92864)),
    Map("node" -> UnorderedDiscrete("4"), "rack" -> UnorderedDiscrete("2"), "time" -> DateTimeStamp(DateTime.parse("2016-08-11T03:31:20.000Z").getMillis), "flops" -> OrderedDiscrete(142092)),
    Map("node" -> UnorderedDiscrete("4"), "rack" -> UnorderedDiscrete("2"), "time" -> DateTimeStamp(DateTime.parse("2016-08-11T03:32:20.000Z").getMillis), "flops" -> OrderedDiscrete(177369)),
    Map("node" -> UnorderedDiscrete("1"), "rack" -> UnorderedDiscrete("1"), "time" -> DateTimeStamp(DateTime.parse("2016-08-11T03:30:00.000Z").getMillis), "flops" -> OrderedDiscrete(23334)),
    Map("node" -> UnorderedDiscrete("1"), "rack" -> UnorderedDiscrete("1"), "time" -> DateTimeStamp(DateTime.parse("2016-08-11T03:31:20.000Z").getMillis), "flops" -> OrderedDiscrete(45523)),
    Map("node" -> UnorderedDiscrete("1"), "rack" -> UnorderedDiscrete("1"), "time" -> DateTimeStamp(DateTime.parse("2016-08-11T03:32:20.000Z").getMillis), "flops" -> OrderedDiscrete(219126)),
    Map("node" -> UnorderedDiscrete("5"), "rack" -> UnorderedDiscrete("2"), "time" -> DateTimeStamp(DateTime.parse("2016-08-11T03:30:00.000Z").getMillis), "flops" -> OrderedDiscrete(22884)),
    Map("node" -> UnorderedDiscrete("5"), "rack" -> UnorderedDiscrete("2"), "time" -> DateTimeStamp(DateTime.parse("2016-08-11T03:31:20.000Z").getMillis), "flops" -> OrderedDiscrete(52343)),
    Map("node" -> UnorderedDiscrete("5"), "rack" -> UnorderedDiscrete("2"), "time" -> DateTimeStamp(DateTime.parse("2016-08-11T03:32:20.000Z").getMillis), "flops" -> OrderedDiscrete(102535)),
    Map("node" -> UnorderedDiscrete("2"), "rack" -> UnorderedDiscrete("1"), "time" -> DateTimeStamp(DateTime.parse("2016-08-11T03:30:00.000Z").getMillis), "flops" -> OrderedDiscrete(35225)),
    Map("node" -> UnorderedDiscrete("2"), "rack" -> UnorderedDiscrete("1"), "time" -> DateTimeStamp(DateTime.parse("2016-08-11T03:31:20.000Z").getMillis), "flops" -> OrderedDiscrete(45417)),
    Map("node" -> UnorderedDiscrete("2"), "rack" -> UnorderedDiscrete("1"), "time" -> DateTimeStamp(DateTime.parse("2016-08-11T03:32:20.000Z").getMillis), "flops" -> OrderedDiscrete(89912)),
    Map("node" -> UnorderedDiscrete("6"), "rack" -> UnorderedDiscrete("2"), "time" -> DateTimeStamp(DateTime.parse("2016-08-11T03:30:00.000Z").getMillis), "flops" -> OrderedDiscrete(5465)),
    Map("node" -> UnorderedDiscrete("6"), "rack" -> UnorderedDiscrete("2"), "time" -> DateTimeStamp(DateTime.parse("2016-08-11T03:31:20.000Z").getMillis), "flops" -> OrderedDiscrete(36378)),
    Map("node" -> UnorderedDiscrete("6"), "rack" -> UnorderedDiscrete("2"), "time" -> DateTimeStamp(DateTime.parse("2016-08-11T03:32:20.000Z").getMillis), "flops" -> OrderedDiscrete(68597)),
    Map("node" -> UnorderedDiscrete("3"), "rack" -> UnorderedDiscrete("1"), "time" -> DateTimeStamp(DateTime.parse("2016-08-11T03:30:00.000Z").getMillis), "flops" -> OrderedDiscrete(1099)),
    Map("node" -> UnorderedDiscrete("3"), "rack" -> UnorderedDiscrete("1"), "time" -> DateTimeStamp(DateTime.parse("2016-08-11T03:31:20.000Z").getMillis), "flops" -> OrderedDiscrete(25437)),
    Map("node" -> UnorderedDiscrete("3"), "rack" -> UnorderedDiscrete("1"), "time" -> DateTimeStamp(DateTime.parse("2016-08-11T03:32:20.000Z").getMillis), "flops" -> OrderedDiscrete(66482))
  )
}
