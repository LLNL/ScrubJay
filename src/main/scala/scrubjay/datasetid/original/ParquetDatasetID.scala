package scrubjay.datasetid.original

import org.apache.spark.sql.{DataFrame, SparkSession}
import scrubjay.dataspace.DimensionSpace
import scrubjay.schema.{ScrubJaySchema, SparkSchema}

case class ParquetDatasetID(parquetFileName: String,
                            originalScrubJaySchema: ScrubJaySchema,
                            sparkSchema: SparkSchema)
  extends  OriginalDatasetID("Parquet", originalScrubJaySchema) {

  override val valid: Boolean = true

  override def originalDF: DataFrame = {
    spark.read.schema(sparkSchema).parquet(parquetFileName)
  }
}
