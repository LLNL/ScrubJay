package scrubjay.datasetid.original

import org.apache.spark.sql.{DataFrame, Dataset}
import scrubjay.datasetid.{ScrubJaySchema}
import scrubjay.dataspace.DimensionSpace

case class LocalDatasetID(dataframe: DataFrame,
                          scrubJaySchema: ScrubJaySchema)
  extends OriginalDatasetID(scrubJaySchema) {

  override def isValid(dimensionSpace: DimensionSpace): Boolean = true

  override def load: DataFrame = {
    dataframe
  }
}

object LocalDatasetID {

}