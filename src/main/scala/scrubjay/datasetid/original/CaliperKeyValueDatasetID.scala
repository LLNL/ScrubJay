package scrubjay.datasetid.original

import org.apache.spark.sql.DataFrame
import scrubjay.dataspace.DimensionSpace
import scrubjay.schema.{ScrubJaySchema, SparkSchema}

case class CaliperKeyValueDatasetID(ckvFileName: String,
                                    sparkSchema: SparkSchema,
                                    originalScrubJaySchema: ScrubJaySchema)
  extends OriginalDatasetID("Caliper", originalScrubJaySchema) {

  override val valid: Boolean = true

  override def originalDF: DataFrame = ???
}
