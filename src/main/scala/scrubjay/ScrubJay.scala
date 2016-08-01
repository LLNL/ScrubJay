package scrubjay

import scrubjay.meta.MetaBase.META_BASE

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext

case class CassandraConnection(
  hostname: String = "localhost",
  username: String = "cassandra",
  password: String = "cassandra") {
    val spark_conf_fields = Array(
      ("spark.cassandra.connection.host", hostname),
      ("spark.cassandra.auth.username", username),
      ("spark.cassandra.auth.password", password),
      ("spark.cassandra.output.ignoreNulls", "true"))
}

class ScrubJaySession(
  spark_master: String = "local[*]",
  cassandra_connection: Option[CassandraConnection] = None) {

  val sparkConf = new SparkConf(true)
    .set("spark.app.id", "ScrubJayAppID")

  // Set cassandra connection fields for spark-cassandra-connector
  //   if parameter is specified (map doesn't run for None values)
  cassandra_connection.map(_.spark_conf_fields.map{
    case (field, value) => sparkConf.set(field, value)})

  val sc = new SparkContext(spark_master, "ScrubJay", sparkConf)
  val sqlContext = new SQLContext(sc)
  val metaOntology = META_BASE
}
