sc.stop

import scrubjay._
import scrubjay.meta._
import scrubjay.datasource._

import scrubjay.datasource.LocalDataSource._
import scrubjay.datasource.CSVDataSource._
import scrubjay.datasource.CassandraDataSource._


import com.datastax.spark.connector._

val cassandraHost = sys.env.get("CQLSH_HOST").getOrElse("localhost")

val sjs = new ScrubJaySession(
  cassandra_connection = Some(CassandraConnection(hostname = cassandraHost)))

println("ScrubJaySession available as sjs")
