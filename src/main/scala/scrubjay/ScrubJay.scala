package scrubjay

import scrubjay.meta._

// Spark
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext

// Datastax
import com.datastax.spark.connector._

case class CassandraConnection(
  val hostname: String = "localhost", 
  val username: String = "cassandra", 
  val password: String = "cassandra") {
    val spark_conf_fields = Array(
      ("spark.cassandra.connection.host", hostname),
      ("spark.cassandra.auth.username", username),
      ("spark.cassandra.auth.password", password),
      ("spark.cassandra.output.ignoreNulls", "true"))
}

class ScrubJaySession(
  //meta_file: String,
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
  val metaOntology = new MetaOntology
}
