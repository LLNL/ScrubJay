package scrubjay.datasource

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}

case class CassandraDataset(keyspace: String,
                            table: String,
                            schema: StructType)
  extends DatasetID {

  // TODO: how to merge incoming schema with schema gathered from Cassandra

  override def isValid: Boolean = true

  override def realize: DataFrame = {
    SparkSession.builder().getOrCreate()
      .read
      .format("org.apache.spark.sql.cassandra")
      .load()
  }
}

object CassandraDataset {

}
