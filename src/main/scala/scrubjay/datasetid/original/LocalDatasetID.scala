package scrubjay.datasetid.original

import org.apache.spark.sql.{DataFrame, Dataset}
import scrubjay.datasetid.{ScrubJaySchema, SparkSchema}
import scrubjay.dataspace.DimensionSpace

case class LocalDatasetID(dataframe: DataFrame,
                          scrubJaySchema: ScrubJaySchema,
                         sparkSchema: Option[SparkSchema] = None)
  extends OriginalDatasetID(scrubJaySchema) {

  override def isValid(dimensionSpace: DimensionSpace): Boolean = true

  override def load: DataFrame = {
    if (sparkSchema.isDefined) {
      spark.createDataFrame(dataframe.rdd, sparkSchema.get)
    } else {
      dataframe
    }
  }
}

object LocalDatasetID {

}