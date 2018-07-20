package scrubjay.datasetid.original

import org.apache.spark.sql.{DataFrame, SparkSession}
import scrubjay.dataspace.DimensionSpace
import scrubjay.schema.{ScrubJaySchema, SparkSchema}

case class ParquetDatasetID(parquetFileName: String,
                            scrubJaySchema: ScrubJaySchema,
                            sparkSchema: SparkSchema)
  extends  OriginalDatasetID(scrubJaySchema) {

  override def isValid(dimensionSpace: DimensionSpace): Boolean = true

  override def load: DataFrame = {
    spark.read.schema(sparkSchema).parquet(parquetFileName)
  }
}
