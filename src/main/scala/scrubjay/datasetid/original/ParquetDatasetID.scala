package scrubjay.datasetid.original

import org.apache.spark.sql.{DataFrame, SparkSession}
import scrubjay.datasetid.{ScrubJaySchema, SparkSchema}
import scrubjay.dataspace.DimensionSpace

case class ParquetDatasetID(parquetFileName: String,
                            scrubJaySchema: ScrubJaySchema,
                            sparkSchema: SparkSchema)
  extends  OriginalDatasetID(scrubJaySchema) {

  override def isValid(dimensionSpace: DimensionSpace): Boolean = true

  override def load: DataFrame = {
    spark.read.schema(sparkSchema).parquet(parquetFileName)
  }
}
