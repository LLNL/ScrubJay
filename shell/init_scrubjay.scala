sc.stop

import scrubjay._
import scrubjay.datasource._

import com.datastax.spark.connector._

val sjs = new ScrubJaySession(
  cassandra_connection = Some(CassandraConnection(hostname="sonar11")))

println("ScrubJaySession available as sjs")
