package scrubjay.datasetid.original

import org.apache.spark.sql.DataFrame
import scrubjay.dataspace.DimensionSpace
import scrubjay.schema.{ScrubJaySchema, SparkSchema}


case class CassandraDatasetID(keyspace: String,
                              table: String,
                              scrubJaySchema: ScrubJaySchema,
                              sparkSchema: SparkSchema)
  extends OriginalDatasetID("CassandraTable", scrubJaySchema) {

  override def isValid(dimensionSpace: DimensionSpace = DimensionSpace.unknown): Boolean = true

  override def load: DataFrame = {
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
