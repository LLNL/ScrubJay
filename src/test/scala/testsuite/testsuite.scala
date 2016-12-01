import scrubjay._
import scrubjay.units._

import org.joda.time.{DateTime, Interval}

package object testsuite {

  /*
   * Columns
   */

  val jobQueueColumns = Seq(
    "jobid",
    "nodelist",
    "elapsed",
    "start",
    "end"
  )

  val clusterLayoutColumns = Seq(
    "node",
    "rack"
  )

  val nodeDataColumns = Seq(
    "node",
    "time",
    "flops"
  )

  /*
   * Raw data
   */

  val jobQueueRawData = Seq(
     Map(
       "jobid"    -> "123",
       "nodelist" -> "1,2,3",
       "elapsed"  -> "23",
       "start"    -> "2016-08-11T3:30:00+0000",
       "end"      -> "2016-08-11T3:31:00+0000"
     ),
     Map(
       "jobid"    -> 456,
       "nodelist" -> List(4, 5, 6),
       "elapsed"  -> 45,
       "start"    -> "2016-08-11T3:30:00+0000",
       "end"      -> "2016-08-11T3:32:00+0000"
     )
  )

  val clusterLayoutRawData = Seq(
    Map("node" -> 1, "rack" -> 1),
    Map("node" -> 2, "rack" -> 1),
    Map("node" -> 3, "rack" -> 1),
    Map("node" -> 4, "rack" -> 2),
    Map("node" -> 5, "rack" -> 2),
    Map("node" -> 6, "rack" -> 2)
  )

  val nodeDataRawData = Seq(
    Map("node" -> 1, "time" -> "2016-08-11T3:30:00+0000", "flops" -> 23334),
    Map("node" -> 1, "time" -> "2016-08-11T3:31:20+0000", "flops" -> 45523),
    Map("node" -> 1, "time" -> "2016-08-11T3:32:20+0000", "flops" -> 219126),
    Map("node" -> 2, "time" -> "2016-08-11T3:30:00+0000", "flops" -> 35225),
    Map("node" -> 2, "time" -> "2016-08-11T3:31:20+0000", "flops" -> 45417),
    Map("node" -> 2, "time" -> "2016-08-11T3:32:20+0000", "flops" -> 89912),
    Map("node" -> 3, "time" -> "2016-08-11T3:30:00+0000", "flops" -> 1099),
    Map("node" -> 3, "time" -> "2016-08-11T3:31:20+0000", "flops" -> 25437),
    Map("node" -> 3, "time" -> "2016-08-11T3:32:20+0000", "flops" -> 66482),
    Map("node" -> 4, "time" -> "2016-08-11T3:30:00+0000", "flops" -> 92864),
    Map("node" -> 4, "time" -> "2016-08-11T3:31:20+0000", "flops" -> 142092),
    Map("node" -> 4, "time" -> "2016-08-11T3:32:20+0000", "flops" -> 177369),
    Map("node" -> 5, "time" -> "2016-08-11T3:30:00+0000", "flops" -> 22884),
    Map("node" -> 5, "time" -> "2016-08-11T3:31:20+0000", "flops" -> 52343),
    Map("node" -> 5, "time" -> "2016-08-11T3:32:20+0000", "flops" -> 102535),
    Map("node" -> 6, "time" -> "2016-08-11T3:30:00+0000", "flops" -> 5465),
    Map("node" -> 6, "time" -> "2016-08-11T3:31:20+0000", "flops" -> 36378),
    Map("node" -> 6, "time" -> "2016-08-11T3:32:20+0000", "flops" -> 68597)
  )

  val jobQueueMeta = Map(
      "jobid" -> metaEntryFromStrings("job", "job", "identifier"),
      "nodelist" -> metaEntryFromStrings("node", "node", "list<identifier>"),
      "elapsed" -> metaEntryFromStrings("duration", "time", "seconds"),
      "start" -> metaEntryFromStrings("start", "time", "datetimestamp"),
      "end" -> metaEntryFromStrings("end", "time", "datetimestamp")
    )

  val nodeDataMeta = Map(
    "node" -> metaEntryFromStrings("node", "node", "identifier"),
    "time" -> metaEntryFromStrings("instant", "time", "datetimestamp"),
    "flops" -> metaEntryFromStrings("cumulative", "flops", "count")
  )

  val clusterLayoutMeta = Map(
    "node" -> metaEntryFromStrings("node", "node", "identifier"),
    "rack" -> metaEntryFromStrings("rack", "rack", "identifier")
  )

  val trueJobQueue = Set(
    Map(
      "jobid" -> UnorderedDiscrete("456"),
      "elapsed" -> Seconds(45.0),
      "end" -> DateTimeStamp(DateTime.parse("2016-08-11T03:32:00.000Z")),
      "start" -> DateTimeStamp(DateTime.parse("2016-08-11T03:30:00.000Z")),
      "nodelist" -> UnitsList(List(UnorderedDiscrete("4"), UnorderedDiscrete("5"), UnorderedDiscrete("6")))
    ),
    Map(
      "jobid" -> UnorderedDiscrete("123"),
      "elapsed" -> Seconds(23.0),
      "end" -> DateTimeStamp(DateTime.parse("2016-08-11T03:31:00.000Z")),
      "start" -> DateTimeStamp(DateTime.parse("2016-08-11T03:30:00.000Z")),
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
     "span"-> DateTimeSpan(new Interval(DateTime.parse("2016-08-11T03:30:00.000Z"), DateTime.parse("2016-08-11T03:32:00.000Z"))),
     "jobid" -> UnorderedDiscrete("456"),
     "elapsed" -> Seconds(45.0),
     "nodelist" -> UnitsList(List(UnorderedDiscrete("4"), UnorderedDiscrete("5"), UnorderedDiscrete("6")))),
    Map(
      "span" -> DateTimeSpan(new Interval(DateTime.parse("2016-08-11T03:30:00.000Z"),DateTime.parse("2016-08-11T03:31:00.000Z"))),
      "jobid" -> UnorderedDiscrete("123"),
      "elapsed" -> Seconds(23.0),
      "nodelist" -> UnitsList(List(UnorderedDiscrete("1"), UnorderedDiscrete("2"), UnorderedDiscrete("3")))
    )
  )

  val trueJobQueueSpanExploded = Set(
    Map(
      "span"-> DateTimeSpan(new Interval(DateTime.parse("2016-08-11T03:30:00.000Z"),DateTime.parse("2016-08-11T03:32:00.000Z"))),
      "jobid" -> UnorderedDiscrete("456"),
      "elapsed" -> Seconds(45.0),
      "nodelist_exploded" -> UnorderedDiscrete("4")),
    Map(
      "span"-> DateTimeSpan(new Interval(DateTime.parse("2016-08-11T03:30:00.000Z"),DateTime.parse("2016-08-11T03:32:00.000Z"))),
      "jobid" -> UnorderedDiscrete("456"),
      "elapsed" -> Seconds(45.0),
      "nodelist_exploded" -> UnorderedDiscrete("5")),
    Map(
      "span"-> DateTimeSpan(new Interval(DateTime.parse("2016-08-11T03:30:00.000Z"),DateTime.parse("2016-08-11T03:32:00.000Z"))),
      "jobid" -> UnorderedDiscrete("456"),
      "elapsed" -> Seconds(45.0),
      "nodelist_exploded" -> UnorderedDiscrete("6")),
    Map(
      "span" -> DateTimeSpan(new Interval(DateTime.parse("2016-08-11T03:30:00.000Z"),DateTime.parse("2016-08-11T03:31:00.000Z"))),
      "jobid" -> UnorderedDiscrete("123"),
      "elapsed" -> Seconds(23.0),
      "nodelist_exploded" -> UnorderedDiscrete("1")),
    Map(
      "span" -> DateTimeSpan(new Interval(DateTime.parse("2016-08-11T03:30:00.000Z"),DateTime.parse("2016-08-11T03:31:00.000Z"))),
      "jobid" -> UnorderedDiscrete("123"),
      "elapsed" -> Seconds(23.0),
      "nodelist_exploded" -> UnorderedDiscrete("2")),
    Map(
      "span" -> DateTimeSpan(new Interval(DateTime.parse("2016-08-11T03:30:00.000Z"),DateTime.parse("2016-08-11T03:31:00.000Z"))),
      "jobid" -> UnorderedDiscrete("123"),
      "elapsed" -> Seconds(23.0),
      "nodelist_exploded" -> UnorderedDiscrete("3"))
  )

  val trueJobQueueSpanExplodedJoined = Set(
    Map(
      "span"-> DateTimeSpan(new Interval(DateTime.parse("2016-08-11T03:30:00.000Z"),DateTime.parse("2016-08-11T03:32:00.000Z"))),
      "jobid" -> UnorderedDiscrete("456"),
      "elapsed" -> Seconds(45.0),
      "rack" -> UnorderedDiscrete("2"),
      "nodelist_exploded" -> UnorderedDiscrete("4")),
    Map(
      "span"-> DateTimeSpan(new Interval(DateTime.parse("2016-08-11T03:30:00.000Z"),DateTime.parse("2016-08-11T03:32:00.000Z"))),
      "jobid" -> UnorderedDiscrete("456"),
      "elapsed" -> Seconds(45.0),
      "rack" -> UnorderedDiscrete("2"),
      "nodelist_exploded" -> UnorderedDiscrete("5")),
    Map(
      "span"-> DateTimeSpan(new Interval(DateTime.parse("2016-08-11T03:30:00.000Z"),DateTime.parse("2016-08-11T03:32:00.000Z"))),
      "jobid" -> UnorderedDiscrete("456"),
      "elapsed" -> Seconds(45.0),
      "rack" -> UnorderedDiscrete("2"),
      "nodelist_exploded" -> UnorderedDiscrete("6")),
    Map(
      "span" -> DateTimeSpan(new Interval(DateTime.parse("2016-08-11T03:30:00.000Z"),DateTime.parse("2016-08-11T03:31:00.000Z"))),
      "jobid" -> UnorderedDiscrete("123"),
      "elapsed" -> Seconds(23.0),
      "rack" -> UnorderedDiscrete("1"),
      "nodelist_exploded" -> UnorderedDiscrete("1")),
    Map(
      "span" -> DateTimeSpan(new Interval(DateTime.parse("2016-08-11T03:30:00.000Z"),DateTime.parse("2016-08-11T03:31:00.000Z"))),
      "jobid" -> UnorderedDiscrete("123"),
      "elapsed" -> Seconds(23.0),
      "rack" -> UnorderedDiscrete("1"),
      "nodelist_exploded" -> UnorderedDiscrete("2")),
    Map(
      "span" -> DateTimeSpan(new Interval(DateTime.parse("2016-08-11T03:30:00.000Z"),DateTime.parse("2016-08-11T03:31:00.000Z"))),
      "jobid" -> UnorderedDiscrete("123"),
      "elapsed" -> Seconds(23.0),
      "rack" -> UnorderedDiscrete("1"),
      "nodelist_exploded" -> UnorderedDiscrete("3"))
  )

  val trueJobQueueSpanExplodedJoinedFlops = Set(
    Map(
      "span" -> DateTimeSpan(new Interval(DateTime.parse("2016-08-11T03:30:00.000Z"),DateTime.parse("2016-08-11T03:32:00.000Z"))),
      "jobid" -> UnorderedDiscrete("456"),
      "elapsed" -> Seconds(45.0),
      "flops" -> OrderedDiscrete(37614),
      "nodelist_exploded" -> UnorderedDiscrete("5"),
      "rack" -> UnorderedDiscrete("2")),
    Map(
      "span" -> DateTimeSpan(new Interval(DateTime.parse("2016-08-11T03:30:00.000Z"),DateTime.parse("2016-08-11T03:32:00.000Z"))),
      "jobid" -> UnorderedDiscrete("456"),
      "elapsed" -> Seconds(45.0),
      "flops" -> OrderedDiscrete(20922),
      "nodelist_exploded" -> UnorderedDiscrete("6"),
      "rack" -> UnorderedDiscrete("2")),
    Map(
      "span" -> DateTimeSpan(new Interval(DateTime.parse("2016-08-11T03:30:00.000Z"),DateTime.parse("2016-08-11T03:31:00.000Z"))),
      "jobid" -> UnorderedDiscrete("123"),
      "elapsed" -> Seconds(23.0),
      "flops" -> OrderedDiscrete(23334),
      "nodelist_exploded" -> UnorderedDiscrete("1"),
      "rack" -> UnorderedDiscrete("1")),
    Map(
      "span" -> DateTimeSpan(new Interval(DateTime.parse("2016-08-11T03:30:00.000Z"),DateTime.parse("2016-08-11T03:31:00.000Z"))),
      "jobid" -> UnorderedDiscrete("123"),
      "elapsed" -> Seconds(23.0),
      "flops" -> OrderedDiscrete(1099),
      "nodelist_exploded" -> UnorderedDiscrete("3"),
      "rack" -> UnorderedDiscrete("1")),
    Map(
      "span" -> DateTimeSpan(new Interval(DateTime.parse("2016-08-11T03:30:00.000Z"),DateTime.parse("2016-08-11T03:32:00.000Z"))),
      "jobid" -> UnorderedDiscrete("456"),
      "elapsed" -> Seconds(45.0),
      "flops" -> OrderedDiscrete(117478),
      "nodelist_exploded" -> UnorderedDiscrete("4"),
      "rack" -> UnorderedDiscrete("2")),
    Map(
      "span" -> DateTimeSpan(new Interval(DateTime.parse("2016-08-11T03:30:00.000Z"),DateTime.parse("2016-08-11T03:31:00.000Z"))),
      "jobid" -> UnorderedDiscrete("123"),
      "elapsed" -> Seconds(23.0),
      "flops" -> OrderedDiscrete(35225),
      "nodelist_exploded" -> UnorderedDiscrete("2"),
      "rack" -> UnorderedDiscrete("1"))
  )

  val trueNodeDataJoinedWithClusterLayout = Set(
    Map("node" -> UnorderedDiscrete("4"), "rack" -> UnorderedDiscrete("2"), "time" -> DateTimeStamp(DateTime.parse("2016-08-11T03:30:00.000Z")), "flops" -> OrderedDiscrete(92864)),
    Map("node" -> UnorderedDiscrete("4"), "rack" -> UnorderedDiscrete("2"), "time" -> DateTimeStamp(DateTime.parse("2016-08-11T03:31:20.000Z")), "flops" -> OrderedDiscrete(142092)),
    Map("node" -> UnorderedDiscrete("4"), "rack" -> UnorderedDiscrete("2"), "time" -> DateTimeStamp(DateTime.parse("2016-08-11T03:32:20.000Z")), "flops" -> OrderedDiscrete(177369)),
    Map("node" -> UnorderedDiscrete("1"), "rack" -> UnorderedDiscrete("1"), "time" -> DateTimeStamp(DateTime.parse("2016-08-11T03:30:00.000Z")), "flops" -> OrderedDiscrete(23334)),
    Map("node" -> UnorderedDiscrete("1"), "rack" -> UnorderedDiscrete("1"), "time" -> DateTimeStamp(DateTime.parse("2016-08-11T03:31:20.000Z")), "flops" -> OrderedDiscrete(45523)),
    Map("node" -> UnorderedDiscrete("1"), "rack" -> UnorderedDiscrete("1"), "time" -> DateTimeStamp(DateTime.parse("2016-08-11T03:32:20.000Z")), "flops" -> OrderedDiscrete(219126)),
    Map("node" -> UnorderedDiscrete("5"), "rack" -> UnorderedDiscrete("2"), "time" -> DateTimeStamp(DateTime.parse("2016-08-11T03:30:00.000Z")), "flops" -> OrderedDiscrete(22884)),
    Map("node" -> UnorderedDiscrete("5"), "rack" -> UnorderedDiscrete("2"), "time" -> DateTimeStamp(DateTime.parse("2016-08-11T03:31:20.000Z")), "flops" -> OrderedDiscrete(52343)),
    Map("node" -> UnorderedDiscrete("5"), "rack" -> UnorderedDiscrete("2"), "time" -> DateTimeStamp(DateTime.parse("2016-08-11T03:32:20.000Z")), "flops" -> OrderedDiscrete(102535)),
    Map("node" -> UnorderedDiscrete("2"), "rack" -> UnorderedDiscrete("1"), "time" -> DateTimeStamp(DateTime.parse("2016-08-11T03:30:00.000Z")), "flops" -> OrderedDiscrete(35225)),
    Map("node" -> UnorderedDiscrete("2"), "rack" -> UnorderedDiscrete("1"), "time" -> DateTimeStamp(DateTime.parse("2016-08-11T03:31:20.000Z")), "flops" -> OrderedDiscrete(45417)),
    Map("node" -> UnorderedDiscrete("2"), "rack" -> UnorderedDiscrete("1"), "time" -> DateTimeStamp(DateTime.parse("2016-08-11T03:32:20.000Z")), "flops" -> OrderedDiscrete(89912)),
    Map("node" -> UnorderedDiscrete("6"), "rack" -> UnorderedDiscrete("2"), "time" -> DateTimeStamp(DateTime.parse("2016-08-11T03:30:00.000Z")), "flops" -> OrderedDiscrete(5465)),
    Map("node" -> UnorderedDiscrete("6"), "rack" -> UnorderedDiscrete("2"), "time" -> DateTimeStamp(DateTime.parse("2016-08-11T03:31:20.000Z")), "flops" -> OrderedDiscrete(36378)),
    Map("node" -> UnorderedDiscrete("6"), "rack" -> UnorderedDiscrete("2"), "time" -> DateTimeStamp(DateTime.parse("2016-08-11T03:32:20.000Z")), "flops" -> OrderedDiscrete(68597)),
    Map("node" -> UnorderedDiscrete("3"), "rack" -> UnorderedDiscrete("1"), "time" -> DateTimeStamp(DateTime.parse("2016-08-11T03:30:00.000Z")), "flops" -> OrderedDiscrete(1099)),
    Map("node" -> UnorderedDiscrete("3"), "rack" -> UnorderedDiscrete("1"), "time" -> DateTimeStamp(DateTime.parse("2016-08-11T03:31:20.000Z")), "flops" -> OrderedDiscrete(25437)),
    Map("node" -> UnorderedDiscrete("3"), "rack" -> UnorderedDiscrete("1"), "time" -> DateTimeStamp(DateTime.parse("2016-08-11T03:32:20.000Z")), "flops" -> OrderedDiscrete(66482))
  )
}
