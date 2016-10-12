sc.stop

import scrubjay.imports._

val cassandraHost = sys.env.get("CQLSH_HOST").getOrElse("localhost")

val sjs = new ScrubJaySession(
  //spark_master = "spark://sonar11:7077",
  cassandra_connection = Some(new CassandraConnection(hostname = cassandraHost)),
  conf_options = Map(
    "spark.driver.memory" -> "128g",
    "spark.executor.memory" -> "128g",
    "spark.driver.maxResultSize" -> "0"))

println("ScrubJaySession available as sjs")
