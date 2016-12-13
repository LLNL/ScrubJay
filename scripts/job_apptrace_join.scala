import scrubjay._

val job_queue_meta = createCassandraMetaSource(sc, "cab_dat_2016", "job_queue_meta")
val job_queue = sc.createCassandraDataSource("cab_dat_2016", "job_queue_before", job_queue_meta, limit=Some(1))
val job_queue_span = job_queue.get.deriveTimeSpan
val job_queue_exploded = job_queue_span.get.deriveExplodeTimeSpan(Seq(("span", org.joda.time.Period.seconds(1))))

val spans = job_queue_span.get.rdd.map(_.get("span").get.value.asInstanceOf[org.joda.time.Interval]).flatMap(i => Seq(i.getStart.getMillis, i.getEnd.getMillis))

val minSec = Math.round(spans.reduce(Math.min) / 1000.0)
val maxSec = Math.round(spans.reduce(Math.max) / 1000.0)

val apptrace_meta = createCassandraMetaSource(sc, "cab_dat_2016", "apptrace_meta")
val apptrace = sc.createCassandraDataSource("cab_dat_2016", "apptrace_before", apptrace_meta, whereConditions=Seq(s""""Timestamp.g" >= $minSec""", s""""Timestamp.g" <= $maxSec"""))

val interjoined = job_queue_exploded.get.deriveInterpolationJoin(apptrace, 1000)
//interjoined.get.rdd.take(5).foreach(println)

val rangejoined = job_queue_span.get.deriveRangeJoin(apptrace)
//rangejoined.get.rdd.take(5)

