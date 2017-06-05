package scrubjay.datasetid.original

import org.apache.spark.sql.DataFrame
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
