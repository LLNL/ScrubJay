sc.stop

import scrubjay._

import org.apache.spark._

val cassandraHost = sys.env.get("CQLSH_HOST").getOrElse("localhost")

val conf = new SparkConf()
  .setMaster("local[*]")
  .setAppName("ScrubJay")
  .set("spark.driver.memory", "128g")
  .set("spark.executor.memory", "128g")
  .set("spark.driver.maxResultSize", "0")
  //.set("spark.cassandra.connection.host", cassandraHost)
  //.set("spark.cassandra.auth.username", "cassandra")
  //.set("spark.cassandra.auth.password", "cassandra")

sc = new SparkContext(conf)

println("ScrubJaySession available as sjs")
