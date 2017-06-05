package scrubjay.datasetid.original

import org.apache.spark.sql.DataFrame
import scrubjay.datasetid.{ScrubJaySchema, SparkSchema}
import scrubjay.dataspace.DimensionSpace


case class CassandraDatasetID(keyspace: String,
                              table: String,
                              sparkSchema: SparkSchema,
                              scrubJaySchema: ScrubJaySchema)
  extends OriginalDatasetID(scrubJaySchema) {

  override def isValid(dimensionSpace: DimensionSpace = DimensionSpace.unknown): Boolean = true

  override def load: DataFrame = {
    spark.read
      .schema(sparkSchema)
      .format("org.apache.spark.sql.cassandra")
      .load()
  }
}

object CassandraDatasetID {
  // TODO: save to cassandra, Cassandra test spec
}
