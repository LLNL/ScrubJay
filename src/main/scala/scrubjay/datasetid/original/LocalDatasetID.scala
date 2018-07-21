package scrubjay.datasetid.original

import org.apache.spark.sql.{DataFrame, Dataset}
import scrubjay.dataspace.DimensionSpace
import scrubjay.schema.{ScrubJaySchema, SparkSchema}

case class LocalDatasetID(dataframe: DataFrame,
                          scrubJaySchema: ScrubJaySchema,
                         sparkSchema: Option[SparkSchema] = None)
  extends OriginalDatasetID("LocalData", scrubJaySchema) {

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