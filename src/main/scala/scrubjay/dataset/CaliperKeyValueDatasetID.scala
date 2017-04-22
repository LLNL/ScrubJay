package scrubjay.dataset

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StructType

case class CaliperKeyValueDatasetID(ckvFileName: String, schema: StructType) extends DatasetID {

  override lazy val isValid: Boolean = true

  override def realize: DataFrame = ???
}
