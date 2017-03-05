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
    Map("jobid" -> UnorderedDiscrete("456"), "elapsed" -> Seconds(45.0), "timespan" -> DateTimeSpan.convert("2016-08-11T03:30:00.000Z,2016-08-11T03:32:00.000Z"), "nodelist" -> UnitsList(List(UnorderedDiscrete("4"), UnorderedDiscrete("5"), UnorderedDiscrete("6"))) ),
    Map( "jobid" -> UnorderedDiscrete("123"), "elapsed" -> Seconds(23.0), "timespan" -> DateTimeSpan.convert("2016-08-11T03:30:00.000Z,2016-08-11T03:31:00.000Z"), "nodelist" -> UnitsList(List(UnorderedDiscrete("1"), UnorderedDiscrete("2"), UnorderedDiscrete("3"))) )
  )

  val trueCabLayout = Set(
    Map("node" -> UnorderedDiscrete("1"), "rack" -> UnorderedDiscrete("1")),
    Map("node" -> UnorderedDiscrete("2"), "rack" -> UnorderedDiscrete("1")),
    Map("node" -> UnorderedDiscrete("3"), "rack" -> UnorderedDiscrete("1")),
    Map("node" -> UnorderedDiscrete("5"), "rack" -> UnorderedDiscrete("2")),
    Map("node" -> UnorderedDiscrete("6"), "rack" -> UnorderedDiscrete("2")),
    Map("node" -> UnorderedDiscrete("4"), "rack" -> UnorderedDiscrete("2"))
  )

  val trueJobQueueExplodedList = Set(
    Map("timespan" -> DateTimeSpan.convert("2016-08-11T03:30:00.000Z,2016-08-11T03:32:00.000Z"), "jobid" -> UnorderedDiscrete("456"), "elapsed" -> Seconds(45.0), "nodelist_exploded" -> UnorderedDiscrete("4")),
    Map("timespan" -> DateTimeSpan.convert("2016-08-11T03:30:00.000Z,2016-08-11T03:32:00.000Z"), "jobid" -> UnorderedDiscrete("456"), "elapsed" -> Seconds(45.0), "nodelist_exploded" -> UnorderedDiscrete("5")),
    Map("timespan" -> DateTimeSpan.convert("2016-08-11T03:30:00.000Z,2016-08-11T03:32:00.000Z"), "jobid" -> UnorderedDiscrete("456"), "elapsed" -> Seconds(45.0), "nodelist_exploded" -> UnorderedDiscrete("6")),
    Map("timespan" -> DateTimeSpan.convert("2016-08-11T03:30:00.000Z,2016-08-11T03:31:00.000Z"), "jobid" -> UnorderedDiscrete("123"), "elapsed" -> Seconds(23.0), "nodelist_exploded" -> UnorderedDiscrete("1")),
    Map("timespan" -> DateTimeSpan.convert("2016-08-11T03:30:00.000Z,2016-08-11T03:31:00.000Z"), "jobid" -> UnorderedDiscrete("123"), "elapsed" -> Seconds(23.0), "nodelist_exploded" -> UnorderedDiscrete("2")),
    Map("timespan" -> DateTimeSpan.convert("2016-08-11T03:30:00.000Z,2016-08-11T03:31:00.000Z"), "jobid" -> UnorderedDiscrete("123"), "elapsed" -> Seconds(23.0), "nodelist_exploded" -> UnorderedDiscrete("3"))
  )

  val trueJobQueueExplodedTime = Set(
    Map("jobid" -> UnorderedDiscrete("123"), "nodelist" -> UnitsList(List(UnorderedDiscrete("1"), UnorderedDiscrete("2"), UnorderedDiscrete("3"))), "elapsed" -> Seconds(23.0), "timespan_exploded" -> DateTimeStamp.convert("2016-08-11T03:30:00.000Z")),
    Map("jobid" -> UnorderedDiscrete("456"), "nodelist" -> UnitsList(List(UnorderedDiscrete("4"), UnorderedDiscrete("5"), UnorderedDiscrete("6"))), "elapsed" -> Seconds(45.0), "timespan_exploded" -> DateTimeStamp.convert("2016-08-11T03:30:00.000Z")),
    Map("jobid" -> UnorderedDiscrete("456"), "nodelist" -> UnitsList(List(UnorderedDiscrete("4"), UnorderedDiscrete("5"), UnorderedDiscrete("6"))), "elapsed" -> Seconds(45.0), "timespan_exploded" -> DateTimeStamp.convert("2016-08-11T03:31:00.000Z"))
  )

  val trueFlopsJoinTemp = Set(
    Map( "node" -> UnorderedDiscrete("1"), "time" -> DateTimeStamp.convert("2016-08-11T3:30:30+0000"), "flops" -> OrderedDiscrete(2000238), "temp" -> DegreesCelsius(45.0) )
  )

  val trueTempJoinFlops = Set(
    Map("node" -> UnorderedDiscrete("1"), "time" -> DateTimeStamp.convert("2016-08-11T3:30:00+0000"), "flops" -> OrderedDiscrete(2000238), "temp" -> DegreesCelsius(40.0)),
    Map("node" -> UnorderedDiscrete("1"), "time" -> DateTimeStamp.convert("2016-08-11T3:31:00+0000"), "flops" -> OrderedDiscrete(2000238), "temp" -> DegreesCelsius(50.0))
  )

  val trueNodeRackTimeFlops = Set(
    Map("node" -> UnorderedDiscrete("4"), "rack" -> UnorderedDiscrete("2"), "time" -> DateTimeStamp.convert("2016-08-11T03:30:00.000Z"), "flops" -> OrderedDiscrete(92864)),
    Map("node" -> UnorderedDiscrete("4"), "rack" -> UnorderedDiscrete("2"), "time" -> DateTimeStamp.convert("2016-08-11T03:31:20.000Z"), "flops" -> OrderedDiscrete(142092)),
    Map("node" -> UnorderedDiscrete("4"), "rack" -> UnorderedDiscrete("2"), "time" -> DateTimeStamp.convert("2016-08-11T03:32:20.000Z"), "flops" -> OrderedDiscrete(177369)),
    Map("node" -> UnorderedDiscrete("1"), "rack" -> UnorderedDiscrete("1"), "time" -> DateTimeStamp.convert("2016-08-11T03:30:00.000Z"), "flops" -> OrderedDiscrete(23334)),
    Map("node" -> UnorderedDiscrete("1"), "rack" -> UnorderedDiscrete("1"), "time" -> DateTimeStamp.convert("2016-08-11T03:31:20.000Z"), "flops" -> OrderedDiscrete(45523)),
    Map("node" -> UnorderedDiscrete("1"), "rack" -> UnorderedDiscrete("1"), "time" -> DateTimeStamp.convert("2016-08-11T03:32:20.000Z"), "flops" -> OrderedDiscrete(219126)),
    Map("node" -> UnorderedDiscrete("5"), "rack" -> UnorderedDiscrete("2"), "time" -> DateTimeStamp.convert("2016-08-11T03:30:00.000Z"), "flops" -> OrderedDiscrete(22884)),
    Map("node" -> UnorderedDiscrete("5"), "rack" -> UnorderedDiscrete("2"), "time" -> DateTimeStamp.convert("2016-08-11T03:31:20.000Z"), "flops" -> OrderedDiscrete(52343)),
    Map("node" -> UnorderedDiscrete("5"), "rack" -> UnorderedDiscrete("2"), "time" -> DateTimeStamp.convert("2016-08-11T03:32:20.000Z"), "flops" -> OrderedDiscrete(102535)),
    Map("node" -> UnorderedDiscrete("2"), "rack" -> UnorderedDiscrete("1"), "time" -> DateTimeStamp.convert("2016-08-11T03:30:00.000Z"), "flops" -> OrderedDiscrete(35225)),
    Map("node" -> UnorderedDiscrete("2"), "rack" -> UnorderedDiscrete("1"), "time" -> DateTimeStamp.convert("2016-08-11T03:31:20.000Z"), "flops" -> OrderedDiscrete(45417)),
    Map("node" -> UnorderedDiscrete("2"), "rack" -> UnorderedDiscrete("1"), "time" -> DateTimeStamp.convert("2016-08-11T03:32:20.000Z"), "flops" -> OrderedDiscrete(89912)),
    Map("node" -> UnorderedDiscrete("6"), "rack" -> UnorderedDiscrete("2"), "time" -> DateTimeStamp.convert("2016-08-11T03:30:00.000Z"), "flops" -> OrderedDiscrete(5465)),
    Map("node" -> UnorderedDiscrete("6"), "rack" -> UnorderedDiscrete("2"), "time" -> DateTimeStamp.convert("2016-08-11T03:31:20.000Z"), "flops" -> OrderedDiscrete(36378)),
    Map("node" -> UnorderedDiscrete("6"), "rack" -> UnorderedDiscrete("2"), "time" -> DateTimeStamp.convert("2016-08-11T03:32:20.000Z"), "flops" -> OrderedDiscrete(68597)),
    Map("node" -> UnorderedDiscrete("3"), "rack" -> UnorderedDiscrete("1"), "time" -> DateTimeStamp.convert("2016-08-11T03:30:00.000Z"), "flops" -> OrderedDiscrete(1099)),
    Map("node" -> UnorderedDiscrete("3"), "rack" -> UnorderedDiscrete("1"), "time" -> DateTimeStamp.convert("2016-08-11T03:31:20.000Z"), "flops" -> OrderedDiscrete(25437)),
    Map("node" -> UnorderedDiscrete("3"), "rack" -> UnorderedDiscrete("1"), "time" -> DateTimeStamp.convert("2016-08-11T03:32:20.000Z"), "flops" -> OrderedDiscrete(66482))
  )

  val trueNodeTimeJobFlops = Set(
    Map("node" -> UnorderedDiscrete("4"), "time" -> DateTimeStamp.convert("2016-08-11T03:30:00.000Z"), "timespan" -> DateTimeSpan.convert("2016-08-11T03:30:00.000Z,2016-08-11T03:32:00.000Z"), "jobid" -> UnorderedDiscrete("456"), "elapsed" -> Seconds(45.0), "flops" -> OrderedDiscrete(92864)),
    Map("node" -> UnorderedDiscrete("4"), "time" -> DateTimeStamp.convert("2016-08-11T03:31:20.000Z"), "timespan" -> DateTimeSpan.convert("2016-08-11T03:30:00.000Z,2016-08-11T03:32:00.000Z"), "jobid" -> UnorderedDiscrete("456"), "elapsed" -> Seconds(45.0), "flops" -> OrderedDiscrete(142092)),
    Map("node" -> UnorderedDiscrete("4"), "time" -> DateTimeStamp.convert("2016-08-11T03:32:20.000Z"), "timespan" -> DateTimeSpan.convert("2016-08-11T03:30:00.000Z,2016-08-11T03:32:00.000Z"), "jobid" -> UnorderedDiscrete("456"), "elapsed" -> Seconds(45.0), "flops" -> OrderedDiscrete(177369)),
    Map("node" -> UnorderedDiscrete("1"), "time" -> DateTimeStamp.convert("2016-08-11T03:30:00.000Z"), "timespan" -> DateTimeSpan.convert("2016-08-11T03:30:00.000Z,2016-08-11T03:31:00.000Z"), "jobid" -> UnorderedDiscrete("123"), "elapsed" -> Seconds(23.0), "flops" -> OrderedDiscrete(23334)),
    Map("node" -> UnorderedDiscrete("1"), "time" -> DateTimeStamp.convert("2016-08-11T03:31:20.000Z"), "timespan" -> DateTimeSpan.convert("2016-08-11T03:30:00.000Z,2016-08-11T03:31:00.000Z"), "jobid" -> UnorderedDiscrete("123"), "elapsed" -> Seconds(23.0), "flops" -> OrderedDiscrete(45523)),
    Map("node" -> UnorderedDiscrete("1"), "time" -> DateTimeStamp.convert("2016-08-11T03:32:20.000Z"), "timespan" -> DateTimeSpan.convert("2016-08-11T03:30:00.000Z,2016-08-11T03:31:00.000Z"), "jobid" -> UnorderedDiscrete("123"), "elapsed" -> Seconds(23.0), "flops" -> OrderedDiscrete(219126)),
    Map("node" -> UnorderedDiscrete("5"), "time" -> DateTimeStamp.convert("2016-08-11T03:30:00.000Z"), "timespan" -> DateTimeSpan.convert("2016-08-11T03:30:00.000Z,2016-08-11T03:32:00.000Z"), "jobid" -> UnorderedDiscrete("456"), "elapsed" -> Seconds(45.0), "flops" -> OrderedDiscrete(22884)),
    Map("node" -> UnorderedDiscrete("5"), "time" -> DateTimeStamp.convert("2016-08-11T03:31:20.000Z"), "timespan" -> DateTimeSpan.convert("2016-08-11T03:30:00.000Z,2016-08-11T03:32:00.000Z"), "jobid" -> UnorderedDiscrete("456"), "elapsed" -> Seconds(45.0), "flops" -> OrderedDiscrete(52343)),
    Map("node" -> UnorderedDiscrete("5"), "time" -> DateTimeStamp.convert("2016-08-11T03:32:20.000Z"), "timespan" -> DateTimeSpan.convert("2016-08-11T03:30:00.000Z,2016-08-11T03:32:00.000Z"), "jobid" -> UnorderedDiscrete("456"), "elapsed" -> Seconds(45.0), "flops" -> OrderedDiscrete(102535)),
    Map("node" -> UnorderedDiscrete("2"), "time" -> DateTimeStamp.convert("2016-08-11T03:30:00.000Z"), "timespan" -> DateTimeSpan.convert("2016-08-11T03:30:00.000Z,2016-08-11T03:31:00.000Z"), "jobid" -> UnorderedDiscrete("123"), "elapsed" -> Seconds(23.0), "flops" -> OrderedDiscrete(35225)),
    Map("node" -> UnorderedDiscrete("2"), "time" -> DateTimeStamp.convert("2016-08-11T03:31:20.000Z"), "timespan" -> DateTimeSpan.convert("2016-08-11T03:30:00.000Z,2016-08-11T03:31:00.000Z"), "jobid" -> UnorderedDiscrete("123"), "elapsed" -> Seconds(23.0), "flops" -> OrderedDiscrete(45417)),
    Map("node" -> UnorderedDiscrete("2"), "time" -> DateTimeStamp.convert("2016-08-11T03:32:20.000Z"), "timespan" -> DateTimeSpan.convert("2016-08-11T03:30:00.000Z,2016-08-11T03:31:00.000Z"), "jobid" -> UnorderedDiscrete("123"), "elapsed" -> Seconds(23.0), "flops" -> OrderedDiscrete(89912)),
    Map("node" -> UnorderedDiscrete("6"), "time" -> DateTimeStamp.convert("2016-08-11T03:30:00.000Z"), "timespan" -> DateTimeSpan.convert("2016-08-11T03:30:00.000Z,2016-08-11T03:32:00.000Z"), "jobid" -> UnorderedDiscrete("456"), "elapsed" -> Seconds(45.0), "flops" -> OrderedDiscrete(5465)),
    Map("node" -> UnorderedDiscrete("6"), "time" -> DateTimeStamp.convert("2016-08-11T03:31:20.000Z"), "timespan" -> DateTimeSpan.convert("2016-08-11T03:30:00.000Z,2016-08-11T03:32:00.000Z"), "jobid" -> UnorderedDiscrete("456"), "elapsed" -> Seconds(45.0), "flops" -> OrderedDiscrete(36378)),
    Map("node" -> UnorderedDiscrete("6"), "time" -> DateTimeStamp.convert("2016-08-11T03:32:20.000Z"), "timespan" -> DateTimeSpan.convert("2016-08-11T03:30:00.000Z,2016-08-11T03:32:00.000Z"), "jobid" -> UnorderedDiscrete("456"), "elapsed" -> Seconds(45.0), "flops" -> OrderedDiscrete(68597)),
    Map("node" -> UnorderedDiscrete("3"), "time" -> DateTimeStamp.convert("2016-08-11T03:30:00.000Z"), "timespan" -> DateTimeSpan.convert("2016-08-11T03:30:00.000Z,2016-08-11T03:31:00.000Z"), "jobid" -> UnorderedDiscrete("123"), "elapsed" -> Seconds(23.0), "flops" -> OrderedDiscrete(1099)),
    Map("node" -> UnorderedDiscrete("3"), "time" -> DateTimeStamp.convert("2016-08-11T03:31:20.000Z"), "timespan" -> DateTimeSpan.convert("2016-08-11T03:30:00.000Z,2016-08-11T03:31:00.000Z"), "jobid" -> UnorderedDiscrete("123"), "elapsed" -> Seconds(23.0), "flops" -> OrderedDiscrete(25437)),
    Map("node" -> UnorderedDiscrete("3"), "time" -> DateTimeStamp.convert("2016-08-11T03:32:20.000Z"), "timespan" -> DateTimeSpan.convert("2016-08-11T03:30:00.000Z,2016-08-11T03:31:00.000Z"), "jobid" -> UnorderedDiscrete("123"), "elapsed" -> Seconds(23.0), "flops" -> OrderedDiscrete(66482))
  )

  val trueNodeRackTimeJobFlops = Set(
    Map("node" -> UnorderedDiscrete("4"), "rack" -> UnorderedDiscrete("2"), "time" -> DateTimeStamp.convert("2016-08-11T03:30:00.000Z"), "timespan" -> DateTimeSpan.convert("2016-08-11T03:30:00.000Z,2016-08-11T03:32:00.000Z"), "jobid" -> UnorderedDiscrete("456"), "elapsed" -> Seconds(45.0), "flops" -> OrderedDiscrete(92864)),
    Map("node" -> UnorderedDiscrete("4"), "rack" -> UnorderedDiscrete("2"), "time" -> DateTimeStamp.convert("2016-08-11T03:31:20.000Z"), "timespan" -> DateTimeSpan.convert("2016-08-11T03:30:00.000Z,2016-08-11T03:32:00.000Z"), "jobid" -> UnorderedDiscrete("456"), "elapsed" -> Seconds(45.0), "flops" -> OrderedDiscrete(142092)),
    Map("node" -> UnorderedDiscrete("4"), "rack" -> UnorderedDiscrete("2"), "time" -> DateTimeStamp.convert("2016-08-11T03:32:20.000Z"), "timespan" -> DateTimeSpan.convert("2016-08-11T03:30:00.000Z,2016-08-11T03:32:00.000Z"), "jobid" -> UnorderedDiscrete("456"), "elapsed" -> Seconds(45.0), "flops" -> OrderedDiscrete(177369)),
    Map("node" -> UnorderedDiscrete("1"), "rack" -> UnorderedDiscrete("1"), "time" -> DateTimeStamp.convert("2016-08-11T03:30:00.000Z"), "timespan" -> DateTimeSpan.convert("2016-08-11T03:30:00.000Z,2016-08-11T03:31:00.000Z"), "jobid" -> UnorderedDiscrete("123"), "elapsed" -> Seconds(23.0), "flops" -> OrderedDiscrete(23334)),
    Map("node" -> UnorderedDiscrete("1"), "rack" -> UnorderedDiscrete("1"), "time" -> DateTimeStamp.convert("2016-08-11T03:31:20.000Z"), "timespan" -> DateTimeSpan.convert("2016-08-11T03:30:00.000Z,2016-08-11T03:31:00.000Z"), "jobid" -> UnorderedDiscrete("123"), "elapsed" -> Seconds(23.0), "flops" -> OrderedDiscrete(45523)),
    Map("node" -> UnorderedDiscrete("1"), "rack" -> UnorderedDiscrete("1"), "time" -> DateTimeStamp.convert("2016-08-11T03:32:20.000Z"), "timespan" -> DateTimeSpan.convert("2016-08-11T03:30:00.000Z,2016-08-11T03:31:00.000Z"), "jobid" -> UnorderedDiscrete("123"), "elapsed" -> Seconds(23.0), "flops" -> OrderedDiscrete(219126)),
    Map("node" -> UnorderedDiscrete("5"), "rack" -> UnorderedDiscrete("2"), "time" -> DateTimeStamp.convert("2016-08-11T03:30:00.000Z"), "timespan" -> DateTimeSpan.convert("2016-08-11T03:30:00.000Z,2016-08-11T03:32:00.000Z"), "jobid" -> UnorderedDiscrete("456"), "elapsed" -> Seconds(45.0), "flops" -> OrderedDiscrete(22884)),
    Map("node" -> UnorderedDiscrete("5"), "rack" -> UnorderedDiscrete("2"), "time" -> DateTimeStamp.convert("2016-08-11T03:31:20.000Z"), "timespan" -> DateTimeSpan.convert("2016-08-11T03:30:00.000Z,2016-08-11T03:32:00.000Z"), "jobid" -> UnorderedDiscrete("456"), "elapsed" -> Seconds(45.0), "flops" -> OrderedDiscrete(52343)),
    Map("node" -> UnorderedDiscrete("5"), "rack" -> UnorderedDiscrete("2"), "time" -> DateTimeStamp.convert("2016-08-11T03:32:20.000Z"), "timespan" -> DateTimeSpan.convert("2016-08-11T03:30:00.000Z,2016-08-11T03:32:00.000Z"), "jobid" -> UnorderedDiscrete("456"), "elapsed" -> Seconds(45.0), "flops" -> OrderedDiscrete(102535)),
    Map("node" -> UnorderedDiscrete("2"), "rack" -> UnorderedDiscrete("1"), "time" -> DateTimeStamp.convert("2016-08-11T03:30:00.000Z"), "timespan" -> DateTimeSpan.convert("2016-08-11T03:30:00.000Z,2016-08-11T03:31:00.000Z"), "jobid" -> UnorderedDiscrete("123"), "elapsed" -> Seconds(23.0), "flops" -> OrderedDiscrete(35225)),
    Map("node" -> UnorderedDiscrete("2"), "rack" -> UnorderedDiscrete("1"), "time" -> DateTimeStamp.convert("2016-08-11T03:31:20.000Z"), "timespan" -> DateTimeSpan.convert("2016-08-11T03:30:00.000Z,2016-08-11T03:31:00.000Z"), "jobid" -> UnorderedDiscrete("123"), "elapsed" -> Seconds(23.0), "flops" -> OrderedDiscrete(45417)),
    Map("node" -> UnorderedDiscrete("2"), "rack" -> UnorderedDiscrete("1"), "time" -> DateTimeStamp.convert("2016-08-11T03:32:20.000Z"), "timespan" -> DateTimeSpan.convert("2016-08-11T03:30:00.000Z,2016-08-11T03:31:00.000Z"), "jobid" -> UnorderedDiscrete("123"), "elapsed" -> Seconds(23.0), "flops" -> OrderedDiscrete(89912)),
    Map("node" -> UnorderedDiscrete("6"), "rack" -> UnorderedDiscrete("2"), "time" -> DateTimeStamp.convert("2016-08-11T03:30:00.000Z"), "timespan" -> DateTimeSpan.convert("2016-08-11T03:30:00.000Z,2016-08-11T03:32:00.000Z"), "jobid" -> UnorderedDiscrete("456"), "elapsed" -> Seconds(45.0), "flops" -> OrderedDiscrete(5465)),
    Map("node" -> UnorderedDiscrete("6"), "rack" -> UnorderedDiscrete("2"), "time" -> DateTimeStamp.convert("2016-08-11T03:31:20.000Z"), "timespan" -> DateTimeSpan.convert("2016-08-11T03:30:00.000Z,2016-08-11T03:32:00.000Z"), "jobid" -> UnorderedDiscrete("456"), "elapsed" -> Seconds(45.0), "flops" -> OrderedDiscrete(36378)),
    Map("node" -> UnorderedDiscrete("6"), "rack" -> UnorderedDiscrete("2"), "time" -> DateTimeStamp.convert("2016-08-11T03:32:20.000Z"), "timespan" -> DateTimeSpan.convert("2016-08-11T03:30:00.000Z,2016-08-11T03:32:00.000Z"), "jobid" -> UnorderedDiscrete("456"), "elapsed" -> Seconds(45.0), "flops" -> OrderedDiscrete(68597)),
    Map("node" -> UnorderedDiscrete("3"), "rack" -> UnorderedDiscrete("1"), "time" -> DateTimeStamp.convert("2016-08-11T03:30:00.000Z"), "timespan" -> DateTimeSpan.convert("2016-08-11T03:30:00.000Z,2016-08-11T03:31:00.000Z"), "jobid" -> UnorderedDiscrete("123"), "elapsed" -> Seconds(23.0), "flops" -> OrderedDiscrete(1099)),
    Map("node" -> UnorderedDiscrete("3"), "rack" -> UnorderedDiscrete("1"), "time" -> DateTimeStamp.convert("2016-08-11T03:31:20.000Z"), "timespan" -> DateTimeSpan.convert("2016-08-11T03:30:00.000Z,2016-08-11T03:31:00.000Z"), "jobid" -> UnorderedDiscrete("123"), "elapsed" -> Seconds(23.0), "flops" -> OrderedDiscrete(25437)),
    Map("node" -> UnorderedDiscrete("3"), "rack" -> UnorderedDiscrete("1"), "time" -> DateTimeStamp.convert("2016-08-11T03:32:20.000Z"), "timespan" -> DateTimeSpan.convert("2016-08-11T03:30:00.000Z,2016-08-11T03:31:00.000Z"), "jobid" -> UnorderedDiscrete("123"), "elapsed" -> Seconds(23.0), "flops" -> OrderedDiscrete(66482))
  )

}
