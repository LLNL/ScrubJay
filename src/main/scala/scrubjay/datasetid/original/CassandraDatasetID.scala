package scrubjay.datasetid.original

import org.apache.spark.sql.DataFrame
import scrubjay.dataspace.DimensionSpace
import scrubjay.schema.{ScrubJaySchema, SparkSchema}


case class CassandraDatasetID(keyspace: String,
                              table: String,
                              originalScrubJaySchema: ScrubJaySchema,
                              sparkSchema: SparkSchema)
  extends OriginalDatasetID("CassandraTable", originalScrubJaySchema) {

  override def isValid: Boolean = true

  override def originalDF: DataFrame = {
    spark.read
      .schema(sparkSchema)
      .format("org.apache.spark.sql.cassandra")
      .options(Map("keyspace" -> keyspace, "table" -> table))
      .load()
  }
}

object CassandraDatasetID {
  // TODO: save to cassandra, Cassandra test spec
}
