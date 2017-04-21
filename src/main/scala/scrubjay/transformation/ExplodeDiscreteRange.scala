package scrubjay.transformation

import scrubjay.dataset._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{explode, udf}
import org.apache.spark.sql.scrubjaytypes.LocalDateTimeRangeType


case class ExplodeDiscreteRange(dsID: DatasetID, column: String)
  extends DatasetID(Seq(dsID)) {

  override lazy val isValid: Boolean = dsID.realize.schema(column).metadata.getString("units").startsWith("list")

  override def realize: DataFrame = {
    val df = dsID.realize
    val string2ListUDF = udf((s: String) => s.split(","))
    val dfList = df.withColumn(column, string2ListUDF(df(column)))
    val dfList2 = dfList.withColumn(column, explode(dfList(column)))
    dfList2
    //df.withColumn(newColumn, explode(df(column)))
  }
}
