package scrubjay.transformation

import scrubjay.datasource._
import scrubjay.units.LocalDateTimeRangeType

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{explode, udf}


case class ExplodeDiscreteRange(dsID: DatasetID, column: String)
  extends DatasetID(dsID) {

  override def isValid: Boolean = dsID.realize.schema(column).metadata.getString("units").startsWith("list")

  override def realize: DataFrame = {
    val df = dsID.realize
    val string2ListUDF = udf((s: String) => s.split(","))
    val dfList = df.withColumn(column, string2ListUDF(df(column)))
    val dfList2 = dfList.withColumn(column, explode(dfList(column)))
    dfList2.withColumn("timespan_parsed", LocalDateTimeRangeType.parseStringUDF(df.schema("timespan").metadata.getString("dateformat"))(df("timespan")))
    //df.withColumn(newColumn, explode(df(column)))
  }
}
