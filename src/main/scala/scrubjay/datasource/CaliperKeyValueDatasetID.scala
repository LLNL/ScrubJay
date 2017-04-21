package scrubjay.datasource

import scrubjay.schema._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StructType

case class CaliperKeyValueDatasetID(ckvFileName: String, schema: StructType) extends DatasetID {

  override def isValid: Boolean = true

  override def realize: DataFrame = ???
}
