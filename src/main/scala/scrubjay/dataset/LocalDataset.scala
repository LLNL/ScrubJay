package scrubjay.dataset

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Row}

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