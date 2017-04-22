package scrubjay.transformation

import scrubjay.dataset._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.explode
import org.apache.spark.sql.types.{ArrayType, DataTypes, MapType}


case class ExplodeDiscreteRange(dsID: DatasetID, column: String)
  extends DatasetID(Seq(dsID)) {

  override lazy val isValid: Boolean = dsID.realize.schema(column).dataType match {
    case ArrayType(_, _) => true
    case MapType(_, _, _) => true
  }

  override def realize: DataFrame = {
    val DF = dsID.realize
    DF.withColumn(column, explode(DF(column)))
  }
}
