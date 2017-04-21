package scrubjay.transformation

import scrubjay.datasource._

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{udf, explode}

/*
 * ExplodeList
 *
 * Requirements: 
 *  1. A single DataSource to derive from
 *  2. A set of user-specified columns, all of which are explode-able rows
 *
 * Derivation:
 *  For every row with an explode-able element,
 *  explodes the row, duplicating all other columns
 */

case class ExplodeDiscreteRange(dsID: DatasetID, column: String)
  extends DatasetID(dsID) {

  override def isValid: Boolean = dsID.schema(column).metadata.getString("units").startsWith("list")

  override def realize: DataFrame = {
    val wowUDF = udf((s: String) => WowString(s))
    val boogersUDF = udf((a: Any) => WowString(a.asInstanceOf[WowString].deriveBoogers))
    val df = dsID.realize
    val string2ListUDF = udf((s: String) => s.split(","))
    val dfList = df.withColumn(column, string2ListUDF(df(column)))
    val dfList2 = dfList.withColumn(column, explode(dfList(column)))
    val dfList3 = dfList2.withColumn(column + "_custom", wowUDF(dfList2(column)))
    dfList3.withColumn(column + "_boogers", boogersUDF(dfList3(column + "_custom")))
    //df.withColumn(newColumn, explode(df(column)))
  }
}
