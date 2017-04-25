package scrubjay.dataset.original

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Row}

case class LocalDatasetID(rawData: Seq[Row], metaSourceID: StructType)
  extends OriginalDatasetID {

  //override val schema: Schema = ???

  override lazy val isValid: Boolean = true

  override def realize: DataFrame = {
    ???
  }
}

object LocalDatasetID {

}