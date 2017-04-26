package scrubjay.dataset.original

import org.apache.spark.sql.DataFrame
import scrubjay.dataset.{ScrubJaySchema, SparkSchema}

case class CaliperKeyValueDatasetID(ckvFileName: String, sparkSchema: SparkSchema, scrubJaySchema: ScrubJaySchema)
  extends OriginalDatasetID {

  override def isValid: Boolean = true

  override def realize: DataFrame = ???
}
