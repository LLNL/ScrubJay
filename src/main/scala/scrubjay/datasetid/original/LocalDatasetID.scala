package scrubjay.datasetid.original

import org.apache.spark.sql.{DataFrame, Dataset}
import scrubjay.datasetid.{ScrubJaySchema, SparkSchema}
import scrubjay.dataspace.DimensionSpace

case class LocalDatasetID(dataframe: DataFrame,
                          scrubJaySchema: ScrubJaySchema)
                         (sparkSchema: SparkSchema = dataframe.schema)
  extends OriginalDatasetID(scrubJaySchema) {

  override def isValid(dimensionSpace: DimensionSpace): Boolean = true

  override def load: DataFrame = {
    if (sparkSchema != dataframe.schema) {
      spark.createDataFrame(dataframe.rdd, sparkSchema)
    } else {
      dataframe
    }
  }
}

object LocalDatasetID {

}