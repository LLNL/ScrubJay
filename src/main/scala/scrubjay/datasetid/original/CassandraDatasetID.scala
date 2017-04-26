package scrubjay.datasetid.original

import org.apache.spark.sql.scrubjayunits.ScrubJayUDFParser
import org.apache.spark.sql.{DataFrame, SparkSession}
import scrubjay.datasetid.{ScrubJaySchema, SparkSchema}

case class CassandraDatasetID(keyspace: String,
                              table: String,
                              sparkSchema: SparkSchema,
                              scrubJaySchema: ScrubJaySchema)
  extends OriginalDatasetID {

  override def isValid: Boolean = true

  override def realize: DataFrame = {
    ScrubJayUDFParser.parse(
      SparkSession.builder().getOrCreate()
        .read
        .schema(sparkSchema)
        .format("org.apache.spark.sql.cassandra")
        .load()
    )
  }
}

object CassandraDatasetID {
  // TODO: save to cassandra, Cassandra test spec
}
