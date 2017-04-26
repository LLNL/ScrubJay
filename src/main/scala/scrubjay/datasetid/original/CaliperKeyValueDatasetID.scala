package scrubjay.datasetid.original

import org.apache.spark.sql.DataFrame
import scrubjay.datasetid.{ScrubJaySchema, SparkSchema}

case class CaliperKeyValueDatasetID(ckvFileName: String, sparkSchema: SparkSchema, scrubJaySchema: ScrubJaySchema)
  extends OriginalDatasetID {

  override def isValid: Boolean = true

  override def realize: DataFrame = ???
}
