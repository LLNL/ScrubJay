sc.stop

import scrubjay._
import scrubjay.datasource._

import com.datastax.spark.connector._

val cassandraHost = sys.env.get("CQLSH_HOST").getOrElse("localhost")

val sjs = new ScrubJaySession(
  cassandra_connection = Some(CassandraConnection(hostname = cassandraHost))

println("ScrubJaySession available as sjs")
