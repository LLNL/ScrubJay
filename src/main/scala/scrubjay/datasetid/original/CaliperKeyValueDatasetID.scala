package scrubjay.datasetid.original

import org.apache.spark.sql.DataFrame
import scrubjay.datasetid.{ScrubJaySchema, SparkSchema}
import scrubjay.dataspace.DimensionSpace

case class CaliperKeyValueDatasetID(ckvFileName: String,
                                    sparkSchema: SparkSchema,
                                    scrubJaySchema: ScrubJaySchema)
  extends OriginalDatasetID(scrubJaySchema) {

  override def isValid(dimensionSpace: DimensionSpace = DimensionSpace.unknown): Boolean = true

  override def load: DataFrame = ???
}
