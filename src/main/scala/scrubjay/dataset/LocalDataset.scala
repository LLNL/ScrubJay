package scrubjay.dataset

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.SQLContext._
import org.apache.spark.sql.types.StructType
import scrubjay.schema._

case class LocalDataset(rawData: Seq[Row], metaSourceID: StructType)
  extends DatasetID {

  //override val schema: Schema = ???

  override lazy val isValid: Boolean = true

  override def realize: DataFrame = {
    ???
  }
}

object LocalDataset {

}