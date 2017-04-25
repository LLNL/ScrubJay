package scrubjay.dataset.original

import org.apache.spark.sql.scrubjaytypes.ScrubJayUDFParser
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}

case class CassandraDatasetID(keyspace: String,
                              table: String,
                              schema: StructType)
  extends OriginalDatasetID {

  override lazy val isValid: Boolean = true

  override def realize: DataFrame = {
    ScrubJayUDFParser.parse(
      SparkSession.builder().getOrCreate()
        .read
        .schema(schema)
        .format("org.apache.spark.sql.cassandra")
        .load()
    )
  }
}

object CassandraDatasetID {
  // TODO: save to cassandra, Cassandra test spec
}
