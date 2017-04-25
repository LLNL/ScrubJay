package scrubjay.dataset.original

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StructType

case class CaliperKeyValueDatasetID(ckvFileName: String, schema: StructType)
  extends OriginalDatasetID {

  override lazy val isValid: Boolean = true

  override def realize: DataFrame = ???
}
