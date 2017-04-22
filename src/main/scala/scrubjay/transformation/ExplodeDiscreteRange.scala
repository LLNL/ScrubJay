package scrubjay.transformation

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.explode
import org.apache.spark.sql.types.{ArrayType, MapType}
import scrubjay.dataset._


case class ExplodeDiscreteRange(dsID: DatasetID, column: String)
  extends DatasetID(Seq(dsID)) {

  override lazy val isValid: Boolean = dsID.realize.schema(column).dataType match {
    case ArrayType(_, _) => true
    case MapType(_, _, _) => true
    case _ => false
  }

  override def realize: DataFrame = {
    val DF = dsID.realize
    DF.withColumn(column, explode(DF(column)))
  }
}
