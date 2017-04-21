package scrubjay.transformation

import scrubjay.datasource._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{explode, udf}
import scrubjay.units.LocalDateTimeRangeType

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

  override def isValid: Boolean = dsID.realize.schema(column).metadata.getString("units").startsWith("list")

  override def realize: DataFrame = {
    val df = dsID.realize
    val string2ListUDF = udf((s: String) => s.split(","))
    val dfList = df.withColumn(column, string2ListUDF(df(column)))
    val dfList2 = dfList.withColumn(column, explode(dfList(column)))

    val explodeTimeRange = udf((tr: LocalDateTimeRangeType) => tr.discretize(30000))
    dfList2.withColumn("timespan_exploded", explodeTimeRange(df("timespan")))
    //df.withColumn(newColumn, explode(df(column)))
  }
}
