package scrubjay.datasetid.original

import org.apache.spark.sql.types.scrubjayunits.ScrubJayDFLoader
import org.apache.spark.sql.{DataFrame, SparkSession}
import scrubjay.datasetid.{ScrubJaySchema, SparkSchema}
import scrubjay.dataspace.DimensionSpace


case class CassandraDatasetID(keyspace: String,
                              table: String,
                              sparkSchema: SparkSchema,
                              scrubJaySchema: ScrubJaySchema)
  extends OriginalDatasetID(scrubJaySchema) {

  override def isValid(dimensionSpace: DimensionSpace = DimensionSpace.empty): Boolean = true

  override def realize(dimensionSpace: DimensionSpace = DimensionSpace.empty): DataFrame = {
    val spark = SparkSession.builder().getOrCreate()
    val rawDF = spark.read
      .schema(sparkSchema)
      .format("org.apache.spark.sql.cassandra")
      .load()
    ScrubJayDFLoader.load(rawDF, scrubJaySchema)
  }
}

object CassandraDatasetID {
  // TODO: save to cassandra, Cassandra test spec
}
