package scrubjay.derivation

import scrubjay.datasource._
import scrubjay.metabase.GlobalMetaBase._
import scrubjay.units._
import scrubjay.util.cartesianProduct

import org.apache.spark.rdd.RDD
import org.joda.time.Period

class ExplodeTimeSpan(dso: Option[DataSource], columnsWithPeriods: Seq[(String, Period)]) extends Transformer(dso) {

  val isValid = columnsWithPeriods.forall(col => ds.metaSource.metaEntryMap(col._1).units == UNITS_DATETIMESPAN)

  def derive: DataSource = new DataSource {

    // Add column_exploded meta entry for each column
    override lazy val metaSource = ds.metaSource.withMetaEntries(
      columnsWithPeriods.map(col => col._1 + "_exploded" -> {
        val originalMetaEntry = ds.metaSource.metaEntryMap(col._1)
        originalMetaEntry.copy(units = UNITS_DATETIMESTAMP)
      }).toMap)
      .withoutColumns(columnsWithPeriods.map(_._1))

    override lazy val rdd: RDD[DataRow] = {

      // Derivation function for flatMap returns a sequence of DataRows
      def derivation(row: DataRow, cols: Seq[(String, Period)]): Seq[DataRow] = {

        // Get lists to explode
        val explodedValues =
        cols.map(col => (col._1, col._2, row(col._1)))
          .map {
            case (k, p, span: DateTimeSpan) => span.explode(p).map(stamp => (k + "_exploded", stamp))
            case (k, p, v) => throw new RuntimeException(s"Runtime type mismatch: \nexpected: DateTimeSpan\nvalue: $v")
          }

        // For multiple expansion columnsWithPeriods, explode into the cartesian product
        val combinations = cartesianProduct(explodedValues)

        // For each combination of exploded values, add a row
        for (combination <- combinations) yield {
          row.filterNot(kv => columnsWithPeriods.map(_._1).contains(kv._1)) ++ Map(combination: _*)
        }
      }

      // Create the derived dataset
      ds.rdd.flatMap(row => derivation(row, columnsWithPeriods))
    }
  }
}

