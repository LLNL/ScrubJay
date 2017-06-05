package scrubjay.datasetid.original

import org.apache.spark.sql.{DataFrame, SparkSession}
import scrubjay.datasetid.ScrubJaySchema
import scrubjay.dataspace.DimensionSpace

case class ParquetDatasetID(parquetFileName: String,
                            scrubJaySchema: ScrubJaySchema)
  extends  OriginalDatasetID(scrubJaySchema) {

  override def isValid(dimensionSpace: DimensionSpace): Boolean = true

  override def load: DataFrame = {
    spark.read.parquet(parquetFileName)
  }
}

object ParquetDatasetID {
  def generateSkeletonFor(parquetFileName: String): ParquetDatasetID = {
    val spark: SparkSession = SparkSession.builder().getOrCreate()
    val df = spark.read.parquet(parquetFileName)
    val scrubJaySchema = ScrubJaySchema.unknown(df.schema)
    ParquetDatasetID(parquetFileName, scrubJaySchema)
  }
}