import scrubjay.units._
import scrubjay.meta._

import com.github.nscala_time.time.Imports._

package object testsuite {

  val jobQueueColumns = Seq(
    "jobid",
    "nodelist",
    "elapsed",
    "start",
    "end"
  )

  val cabLayoutColumns = Seq(
    "node",
    "rack"
  )

  val jobQueueRawData = Seq(
     Map(
       "jobid" -> "123",
       "nodelist" -> "1,2,3",
       "elapsed" -> "23",
       "start" -> "2016-08-11T3:30:00+0000",
       "end" -> "2016-08-11T3:30:23+0000"
     ),
     Map(
       "jobid" -> 456,
       "nodelist" -> List(4, 5, 6),
       "elapsed" -> 45,
       "start" -> "2016-08-11T3:30:20+0000",
       "end" -> "2016-08-11T3:31:05+0000"
     )
  )

  val cabLayoutRawData = Seq(
    Map("node" -> 1, "rack" -> 1),
    Map("node" -> 2, "rack" -> 1),
    Map("node" -> 3, "rack" -> 1),
    Map("node" -> 4, "rack" -> 2),
    Map("node" -> 5, "rack" -> 2),
    Map("node" -> 6, "rack" -> 2)
  )

  val jobQueueMeta = Map(
      "jobid" -> MetaEntry.fromStringTuple("job", "job", "identifier"),
      "nodelist" -> MetaEntry.fromStringTuple("node", "node", "list<identifier>"),
      "elapsed" -> MetaEntry.fromStringTuple("duration", "time", "seconds"),
      "start" -> MetaEntry.fromStringTuple("start", "time", "datetimestamp"),
      "end" -> MetaEntry.fromStringTuple("end", "time", "datetimestamp")
    )

  val cabLayoutMeta = Map(
    "node" -> MetaEntry.fromStringTuple("node", "node", "identifier"),
    "rack" -> MetaEntry.fromStringTuple("rack", "rack", "identifier")
  )

  val trueJobQueue = Set(
    Map(
      "jobid" -> Identifier("456"),
      "elapsed" -> Seconds(45.0),
      "end" -> DateTimeStamp(DateTime.parse("2016-08-11T03:31:05.000Z")),
      "start" -> DateTimeStamp(DateTime.parse("2016-08-11T03:30:20.000Z")),
      "nodelist" -> UnitsList(List(Identifier("4"), Identifier("5"), Identifier("6")))
    ),
    Map(
      "jobid" -> Identifier("123"),
      "elapsed" -> Seconds(23.0),
      "end" -> DateTimeStamp(DateTime.parse("2016-08-11T03:30:23.000Z")),
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
     "span"-> DateTimeSpan(DateTime.parse("2016-08-11T03:30:20.000Z") to DateTime.parse("2016-08-11T03:31:05.000Z")),
     "jobid" -> Identifier("456"),
     "elapsed" -> Seconds(45.0),
     "end" -> DateTimeStamp(DateTime.parse("2016-08-11T03:31:05.000Z")),
     "start" -> DateTimeStamp(DateTime.parse("2016-08-11T03:30:20.000Z")),
     "nodelist" -> UnitsList(List(Identifier("4"), Identifier("5"), Identifier("6")))),
    Map(
      "span" -> DateTimeSpan(DateTime.parse("2016-08-11T03:30:00.000Z") to DateTime.parse("2016-08-11T03:30:23.000Z")),
      "jobid" -> Identifier("123"),
      "elapsed" -> Seconds(23.0),
      "end" -> DateTimeStamp(DateTime.parse("2016-08-11T03:30:23.000Z")),
      "start" -> DateTimeStamp(DateTime.parse("2016-08-11T03:30:00.000Z")),
      "nodelist" -> UnitsList(List(Identifier("1"), Identifier("2"), Identifier("3")))
    )
  )

  val trueJobQueueSpanExpanded = Set(
    Map(
      "span"-> DateTimeSpan(DateTime.parse("2016-08-11T03:30:20.000Z") to DateTime.parse("2016-08-11T03:31:05.000Z")),
      "jobid" -> Identifier("456"),
      "elapsed" -> Seconds(45.0),
      "end" -> DateTimeStamp(DateTime.parse("2016-08-11T03:31:05.000Z")),
      "start" -> DateTimeStamp(DateTime.parse("2016-08-11T03:30:20.000Z")),
      "nodelist" -> UnitsList(List(Identifier("4"), Identifier("5"), Identifier("6"))),
      "nodelist_exploded" -> Identifier("4")),
    Map(
      "span"-> DateTimeSpan(DateTime.parse("2016-08-11T03:30:20.000Z") to DateTime.parse("2016-08-11T03:31:05.000Z")),
      "jobid" -> Identifier("456"),
      "elapsed" -> Seconds(45.0),
      "end" -> DateTimeStamp(DateTime.parse("2016-08-11T03:31:05.000Z")),
      "start" -> DateTimeStamp(DateTime.parse("2016-08-11T03:30:20.000Z")),
      "nodelist" -> UnitsList(List(Identifier("4"), Identifier("5"), Identifier("6"))),
      "nodelist_exploded" -> Identifier("5")),
    Map(
      "span"-> DateTimeSpan(DateTime.parse("2016-08-11T03:30:20.000Z") to DateTime.parse("2016-08-11T03:31:05.000Z")),
      "jobid" -> Identifier("456"),
      "elapsed" -> Seconds(45.0),
      "end" -> DateTimeStamp(DateTime.parse("2016-08-11T03:31:05.000Z")),
      "start" -> DateTimeStamp(DateTime.parse("2016-08-11T03:30:20.000Z")),
      "nodelist" -> UnitsList(List(Identifier("4"), Identifier("5"), Identifier("6"))),
      "nodelist_exploded" -> Identifier("6")),
    Map(
      "span" -> DateTimeSpan(DateTime.parse("2016-08-11T03:30:00.000Z") to DateTime.parse("2016-08-11T03:30:23.000Z")),
      "jobid" -> Identifier("123"),
      "elapsed" -> Seconds(23.0),
      "end" -> DateTimeStamp(DateTime.parse("2016-08-11T03:30:23.000Z")),
      "start" -> DateTimeStamp(DateTime.parse("2016-08-11T03:30:00.000Z")),
      "nodelist" -> UnitsList(List(Identifier("1"), Identifier("2"), Identifier("3"))),
      "nodelist_exploded" -> Identifier("1")),
    Map(
      "span" -> DateTimeSpan(DateTime.parse("2016-08-11T03:30:00.000Z") to DateTime.parse("2016-08-11T03:30:23.000Z")),
      "jobid" -> Identifier("123"),
      "elapsed" -> Seconds(23.0),
      "end" -> DateTimeStamp(DateTime.parse("2016-08-11T03:30:23.000Z")),
      "start" -> DateTimeStamp(DateTime.parse("2016-08-11T03:30:00.000Z")),
      "nodelist" -> UnitsList(List(Identifier("1"), Identifier("2"), Identifier("3"))),
      "nodelist_exploded" -> Identifier("2")),
    Map(
      "span" -> DateTimeSpan(DateTime.parse("2016-08-11T03:30:00.000Z") to DateTime.parse("2016-08-11T03:30:23.000Z")),
      "jobid" -> Identifier("123"),
      "elapsed" -> Seconds(23.0),
      "end" -> DateTimeStamp(DateTime.parse("2016-08-11T03:30:23.000Z")),
      "start" -> DateTimeStamp(DateTime.parse("2016-08-11T03:30:00.000Z")),
      "nodelist" -> UnitsList(List(Identifier("1"), Identifier("2"), Identifier("3"))),
      "nodelist_exploded" -> Identifier("3"))
  )

  val trueJobQueueSpanExpandedJoined = Set(
    Map(
      "span"-> DateTimeSpan(DateTime.parse("2016-08-11T03:30:20.000Z") to DateTime.parse("2016-08-11T03:31:05.000Z")),
      "jobid" -> Identifier("456"),
      "elapsed" -> Seconds(45.0),
      "end" -> DateTimeStamp(DateTime.parse("2016-08-11T03:31:05.000Z")),
      "start" -> DateTimeStamp(DateTime.parse("2016-08-11T03:30:20.000Z")),
      "rack" -> Identifier("2"),
      "nodelist" -> UnitsList(List(Identifier("4"), Identifier("5"), Identifier("6"))),
      "nodelist_exploded" -> Identifier("4")),
    Map(
      "span"-> DateTimeSpan(DateTime.parse("2016-08-11T03:30:20.000Z") to DateTime.parse("2016-08-11T03:31:05.000Z")),
      "jobid" -> Identifier("456"),
      "elapsed" -> Seconds(45.0),
      "end" -> DateTimeStamp(DateTime.parse("2016-08-11T03:31:05.000Z")),
      "start" -> DateTimeStamp(DateTime.parse("2016-08-11T03:30:20.000Z")),
      "rack" -> Identifier("2"),
      "nodelist" -> UnitsList(List(Identifier("4"), Identifier("5"), Identifier("6"))),
      "nodelist_exploded" -> Identifier("5")),
    Map(
      "span"-> DateTimeSpan(DateTime.parse("2016-08-11T03:30:20.000Z") to DateTime.parse("2016-08-11T03:31:05.000Z")),
      "jobid" -> Identifier("456"),
      "elapsed" -> Seconds(45.0),
      "end" -> DateTimeStamp(DateTime.parse("2016-08-11T03:31:05.000Z")),
      "start" -> DateTimeStamp(DateTime.parse("2016-08-11T03:30:20.000Z")),
      "rack" -> Identifier("2"),
      "nodelist" -> UnitsList(List(Identifier("4"), Identifier("5"), Identifier("6"))),
      "nodelist_exploded" -> Identifier("6")),
    Map(
      "span" -> DateTimeSpan(DateTime.parse("2016-08-11T03:30:00.000Z") to DateTime.parse("2016-08-11T03:30:23.000Z")),
      "jobid" -> Identifier("123"),
      "elapsed" -> Seconds(23.0),
      "end" -> DateTimeStamp(DateTime.parse("2016-08-11T03:30:23.000Z")),
      "start" -> DateTimeStamp(DateTime.parse("2016-08-11T03:30:00.000Z")),
      "rack" -> Identifier("1"),
      "nodelist" -> UnitsList(List(Identifier("1"), Identifier("2"), Identifier("3"))),
      "nodelist_exploded" -> Identifier("1")),
    Map(
      "span" -> DateTimeSpan(DateTime.parse("2016-08-11T03:30:00.000Z") to DateTime.parse("2016-08-11T03:30:23.000Z")),
      "jobid" -> Identifier("123"),
      "elapsed" -> Seconds(23.0),
      "end" -> DateTimeStamp(DateTime.parse("2016-08-11T03:30:23.000Z")),
      "start" -> DateTimeStamp(DateTime.parse("2016-08-11T03:30:00.000Z")),
      "rack" -> Identifier("1"),
      "nodelist" -> UnitsList(List(Identifier("1"), Identifier("2"), Identifier("3"))),
      "nodelist_exploded" -> Identifier("2")),
    Map(
      "span" -> DateTimeSpan(DateTime.parse("2016-08-11T03:30:00.000Z") to DateTime.parse("2016-08-11T03:30:23.000Z")),
      "jobid" -> Identifier("123"),
      "elapsed" -> Seconds(23.0),
      "end" -> DateTimeStamp(DateTime.parse("2016-08-11T03:30:23.000Z")),
      "start" -> DateTimeStamp(DateTime.parse("2016-08-11T03:30:00.000Z")),
      "rack" -> Identifier("1"),
      "nodelist" -> UnitsList(List(Identifier("1"), Identifier("2"), Identifier("3"))),
      "nodelist_exploded" -> Identifier("3"))
  )
}
