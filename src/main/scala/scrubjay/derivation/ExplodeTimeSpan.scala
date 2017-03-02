package scrubjay.derivation

import scrubjay.datasource._
import scrubjay.metabase.GlobalMetaBase._
import scrubjay.units._
import scrubjay.metasource._
import scrubjay.util.cartesianProduct

import org.apache.spark.rdd.RDD

class ExplodeTimeSpan(dsID: DataSourceID, columnsWithPeriods: Seq[(String, Double)])
  extends DataSourceID(Seq(dsID))(Seq(columnsWithPeriods)) {

  // Add column_exploded meta entry for each column
  val metaSource: MetaSource = dsID.metaSource.withMetaEntries(
    columnsWithPeriods.map(col => col._1 + "_exploded" -> {
      val originalMetaEntry = dsID.metaSource(col._1)
      originalMetaEntry.copy(units = UNITS_DATETIMESTAMP)
    }).toMap)
    .withoutColumns(columnsWithPeriods.map(_._1))


  def realize: ScrubJayRDD = {

    val ds = dsID.realize

    val rdd: RDD[DataRow] = {

      // Derivation function for flatMap returns a sequence of DataRows
      def derivation(row: DataRow, cols: Seq[(String, Double)]): Seq[DataRow] = {

        // Get lists to explode
        val explodedValues =
        cols.map(col => (col._1, col._2, row(col._1)))
          .map {
            case (k, p, span: DateTimeSpan) => span.explode(p).map(stamp => (k + "_exploded", stamp))
            case (_, _, v) => throw new RuntimeException(s"Runtime type mismatch: \nexpected: DateTimeSpan\nvalue: $v")
          }

        // For multiple expansion columnsWithPeriods, explode into the cartesian product
        val combinations = cartesianProduct(explodedValues)

        // For each combination of exploded values, add a row
        for (combination <- combinations) yield {
          row.filterNot(kv => columnsWithPeriods.map(_._1).contains(kv._1)) ++ Map(combination: _*)
        }
      }

      // Create the derived dataset
      ds.flatMap(row => derivation(row, columnsWithPeriods))
    }

    new ScrubJayRDD(rdd)
  }
}

object ExplodeTimeSpan {
  def apply(dsID: DataSourceID, columnsWithPeriods: Seq[(String, Double)]): Option[DataSourceID] = {
    val isValid: Boolean = columnsWithPeriods.forall(col => dsID.metaSource(col._1).units == UNITS_DATETIMESPAN)
    if (isValid)
      Some(new ExplodeTimeSpan(dsID, columnsWithPeriods))
    else
      None
  }
}
