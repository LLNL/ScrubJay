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
      "jobid" -> Identifier("456"),
      "elapsed" -> Seconds(45.0),
      "end" -> DateTimeStamp(DateTime.parse("2016-08-11T03:32:00.000Z")),
      "start" -> DateTimeStamp(DateTime.parse("2016-08-11T03:30:00.000Z")),
      "nodelist" -> UnitsList(List(Identifier("4"), Identifier("5"), Identifier("6")))
    ),
    Map(
      "jobid" -> Identifier("123"),
      "elapsed" -> Seconds(23.0),
      "end" -> DateTimeStamp(DateTime.parse("2016-08-11T03:31:00.000Z")),
      "start" -> DateTimeStamp(DateTime.parse("2016-08-11T03:30:00.000Z")),
      "nodelist" -> UnitsList(List(Identifier("1"), Identifier("2"), Identifier("3")))
    )
  )

  val trueCabLayout = Set(
    Map("node" -> Identifier("1"), "rack" -> Identifier("1")),
    Map("node" -> Identifier("2"), "rack" -> Identifier("1")),
    Map("node" -> Identifier("3"), "rack" -> Identifier("1")),
    Map("node" -> Identifier("5"), "rack" -> Identifier("2")),
    Map("node" -> Identifier("6"), "rack" -> Identifier("2")),
    Map("node" -> Identifier("4"), "rack" -> Identifier("2"))
  )

  val trueJobQueueSpan = Set(
    Map(
     "span"-> DateTimeSpan(new Interval(DateTime.parse("2016-08-11T03:30:00.000Z"), DateTime.parse("2016-08-11T03:32:00.000Z"))),
     "jobid" -> Identifier("456"),
     "elapsed" -> Seconds(45.0),
     "nodelist" -> UnitsList(List(Identifier("4"), Identifier("5"), Identifier("6")))),
    Map(
      "span" -> DateTimeSpan(new Interval(DateTime.parse("2016-08-11T03:30:00.000Z"),DateTime.parse("2016-08-11T03:31:00.000Z"))),
      "jobid" -> Identifier("123"),
      "elapsed" -> Seconds(23.0),
      "nodelist" -> UnitsList(List(Identifier("1"), Identifier("2"), Identifier("3")))
    )
  )

  val trueJobQueueSpanExploded = Set(
    Map(
      "span"-> DateTimeSpan(new Interval(DateTime.parse("2016-08-11T03:30:00.000Z"),DateTime.parse("2016-08-11T03:32:00.000Z"))),
      "jobid" -> Identifier("456"),
      "elapsed" -> Seconds(45.0),
      "nodelist_exploded" -> Identifier("4")),
    Map(
      "span"-> DateTimeSpan(new Interval(DateTime.parse("2016-08-11T03:30:00.000Z"),DateTime.parse("2016-08-11T03:32:00.000Z"))),
      "jobid" -> Identifier("456"),
      "elapsed" -> Seconds(45.0),
      "nodelist_exploded" -> Identifier("5")),
    Map(
      "span"-> DateTimeSpan(new Interval(DateTime.parse("2016-08-11T03:30:00.000Z"),DateTime.parse("2016-08-11T03:32:00.000Z"))),
      "jobid" -> Identifier("456"),
      "elapsed" -> Seconds(45.0),
      "nodelist_exploded" -> Identifier("6")),
    Map(
      "span" -> DateTimeSpan(new Interval(DateTime.parse("2016-08-11T03:30:00.000Z"),DateTime.parse("2016-08-11T03:31:00.000Z"))),
      "jobid" -> Identifier("123"),
      "elapsed" -> Seconds(23.0),
      "nodelist_exploded" -> Identifier("1")),
    Map(
      "span" -> DateTimeSpan(new Interval(DateTime.parse("2016-08-11T03:30:00.000Z"),DateTime.parse("2016-08-11T03:31:00.000Z"))),
      "jobid" -> Identifier("123"),
      "elapsed" -> Seconds(23.0),
      "nodelist_exploded" -> Identifier("2")),
    Map(
      "span" -> DateTimeSpan(new Interval(DateTime.parse("2016-08-11T03:30:00.000Z"),DateTime.parse("2016-08-11T03:31:00.000Z"))),
      "jobid" -> Identifier("123"),
      "elapsed" -> Seconds(23.0),
      "nodelist_exploded" -> Identifier("3"))
  )

  val trueJobQueueSpanExplodedJoined = Set(
    Map(
      "span"-> DateTimeSpan(new Interval(DateTime.parse("2016-08-11T03:30:00.000Z"),DateTime.parse("2016-08-11T03:32:00.000Z"))),
      "jobid" -> Identifier("456"),
      "elapsed" -> Seconds(45.0),
      "rack" -> Identifier("2"),
      "nodelist_exploded" -> Identifier("4")),
    Map(
      "span"-> DateTimeSpan(new Interval(DateTime.parse("2016-08-11T03:30:00.000Z"),DateTime.parse("2016-08-11T03:32:00.000Z"))),
      "jobid" -> Identifier("456"),
      "elapsed" -> Seconds(45.0),
      "rack" -> Identifier("2"),
      "nodelist_exploded" -> Identifier("5")),
    Map(
      "span"-> DateTimeSpan(new Interval(DateTime.parse("2016-08-11T03:30:00.000Z"),DateTime.parse("2016-08-11T03:32:00.000Z"))),
      "jobid" -> Identifier("456"),
      "elapsed" -> Seconds(45.0),
      "rack" -> Identifier("2"),
      "nodelist_exploded" -> Identifier("6")),
    Map(
      "span" -> DateTimeSpan(new Interval(DateTime.parse("2016-08-11T03:30:00.000Z"),DateTime.parse("2016-08-11T03:31:00.000Z"))),
      "jobid" -> Identifier("123"),
      "elapsed" -> Seconds(23.0),
      "rack" -> Identifier("1"),
      "nodelist_exploded" -> Identifier("1")),
    Map(
      "span" -> DateTimeSpan(new Interval(DateTime.parse("2016-08-11T03:30:00.000Z"),DateTime.parse("2016-08-11T03:31:00.000Z"))),
      "jobid" -> Identifier("123"),
      "elapsed" -> Seconds(23.0),
      "rack" -> Identifier("1"),
      "nodelist_exploded" -> Identifier("2")),
    Map(
      "span" -> DateTimeSpan(new Interval(DateTime.parse("2016-08-11T03:30:00.000Z"),DateTime.parse("2016-08-11T03:31:00.000Z"))),
      "jobid" -> Identifier("123"),
      "elapsed" -> Seconds(23.0),
      "rack" -> Identifier("1"),
      "nodelist_exploded" -> Identifier("3"))
  )

  val trueJobQueueSpanExplodedJoinedFlops = Set(
    Map(
      "span" -> DateTimeSpan(new Interval(DateTime.parse("2016-08-11T03:30:00.000Z"),DateTime.parse("2016-08-11T03:32:00.000Z"))),
      "jobid" -> Identifier("456"),
      "elapsed" -> Seconds(45.0),
      "flops" -> Count(37614),
      "nodelist_exploded" -> Identifier("5"),
      "rack" -> Identifier("2")),
    Map(
      "span" -> DateTimeSpan(new Interval(DateTime.parse("2016-08-11T03:30:00.000Z"),DateTime.parse("2016-08-11T03:32:00.000Z"))),
      "jobid" -> Identifier("456"),
      "elapsed" -> Seconds(45.0),
      "flops" -> Count(20922),
      "nodelist_exploded" -> Identifier("6"),
      "rack" -> Identifier("2")),
    Map(
      "span" -> DateTimeSpan(new Interval(DateTime.parse("2016-08-11T03:30:00.000Z"),DateTime.parse("2016-08-11T03:31:00.000Z"))),
      "jobid" -> Identifier("123"),
      "elapsed" -> Seconds(23.0),
      "flops" -> Count(23334),
      "nodelist_exploded" -> Identifier("1"),
      "rack" -> Identifier("1")),
    Map(
      "span" -> DateTimeSpan(new Interval(DateTime.parse("2016-08-11T03:30:00.000Z"),DateTime.parse("2016-08-11T03:31:00.000Z"))),
      "jobid" -> Identifier("123"),
      "elapsed" -> Seconds(23.0),
      "flops" -> Count(1099),
      "nodelist_exploded" -> Identifier("3"),
      "rack" -> Identifier("1")),
    Map(
      "span" -> DateTimeSpan(new Interval(DateTime.parse("2016-08-11T03:30:00.000Z"),DateTime.parse("2016-08-11T03:32:00.000Z"))),
      "jobid" -> Identifier("456"),
      "elapsed" -> Seconds(45.0),
      "flops" -> Count(117478),
      "nodelist_exploded" -> Identifier("4"),
      "rack" -> Identifier("2")),
    Map(
      "span" -> DateTimeSpan(new Interval(DateTime.parse("2016-08-11T03:30:00.000Z"),DateTime.parse("2016-08-11T03:31:00.000Z"))),
      "jobid" -> Identifier("123"),
      "elapsed" -> Seconds(23.0),
      "flops" -> Count(35225),
      "nodelist_exploded" -> Identifier("2"),
      "rack" -> Identifier("1"))
  )

  val trueNodeDataJoinedWithClusterLayout = Set(
    Map("node" -> Identifier("4"), "rack" -> Identifier("2"), "time" -> DateTimeStamp(DateTime.parse("2016-08-11T03:30:00.000Z")), "flops" -> Count(92864)),
    Map("node" -> Identifier("4"), "rack" -> Identifier("2"), "time" -> DateTimeStamp(DateTime.parse("2016-08-11T03:31:20.000Z")), "flops" -> Count(142092)),
    Map("node" -> Identifier("4"), "rack" -> Identifier("2"), "time" -> DateTimeStamp(DateTime.parse("2016-08-11T03:32:20.000Z")), "flops" -> Count(177369)),
    Map("node" -> Identifier("1"), "rack" -> Identifier("1"), "time" -> DateTimeStamp(DateTime.parse("2016-08-11T03:30:00.000Z")), "flops" -> Count(23334)),
    Map("node" -> Identifier("1"), "rack" -> Identifier("1"), "time" -> DateTimeStamp(DateTime.parse("2016-08-11T03:31:20.000Z")), "flops" -> Count(45523)),
    Map("node" -> Identifier("1"), "rack" -> Identifier("1"), "time" -> DateTimeStamp(DateTime.parse("2016-08-11T03:32:20.000Z")), "flops" -> Count(219126)),
    Map("node" -> Identifier("5"), "rack" -> Identifier("2"), "time" -> DateTimeStamp(DateTime.parse("2016-08-11T03:30:00.000Z")), "flops" -> Count(22884)),
    Map("node" -> Identifier("5"), "rack" -> Identifier("2"), "time" -> DateTimeStamp(DateTime.parse("2016-08-11T03:31:20.000Z")), "flops" -> Count(52343)),
    Map("node" -> Identifier("5"), "rack" -> Identifier("2"), "time" -> DateTimeStamp(DateTime.parse("2016-08-11T03:32:20.000Z")), "flops" -> Count(102535)),
    Map("node" -> Identifier("2"), "rack" -> Identifier("1"), "time" -> DateTimeStamp(DateTime.parse("2016-08-11T03:30:00.000Z")), "flops" -> Count(35225)),
    Map("node" -> Identifier("2"), "rack" -> Identifier("1"), "time" -> DateTimeStamp(DateTime.parse("2016-08-11T03:31:20.000Z")), "flops" -> Count(45417)),
    Map("node" -> Identifier("2"), "rack" -> Identifier("1"), "time" -> DateTimeStamp(DateTime.parse("2016-08-11T03:32:20.000Z")), "flops" -> Count(89912)),
    Map("node" -> Identifier("6"), "rack" -> Identifier("2"), "time" -> DateTimeStamp(DateTime.parse("2016-08-11T03:30:00.000Z")), "flops" -> Count(5465)),
    Map("node" -> Identifier("6"), "rack" -> Identifier("2"), "time" -> DateTimeStamp(DateTime.parse("2016-08-11T03:31:20.000Z")), "flops" -> Count(36378)),
    Map("node" -> Identifier("6"), "rack" -> Identifier("2"), "time" -> DateTimeStamp(DateTime.parse("2016-08-11T03:32:20.000Z")), "flops" -> Count(68597)),
    Map("node" -> Identifier("3"), "rack" -> Identifier("1"), "time" -> DateTimeStamp(DateTime.parse("2016-08-11T03:30:00.000Z")), "flops" -> Count(1099)),
    Map("node" -> Identifier("3"), "rack" -> Identifier("1"), "time" -> DateTimeStamp(DateTime.parse("2016-08-11T03:31:20.000Z")), "flops" -> Count(25437)),
    Map("node" -> Identifier("3"), "rack" -> Identifier("1"), "time" -> DateTimeStamp(DateTime.parse("2016-08-11T03:32:20.000Z")), "flops" -> Count(66482))
  )
}
