package scrubjay.derivation

import scrubjay.datasource._
import scrubjay.metabase.GlobalMetaBase._
import scrubjay.units._
import scrubjay.util.cartesianProduct

import org.apache.spark.rdd.RDD

/*
 * ExplodeList
 *
 * Requirements: 
 *  1. A single DataSource to derive from
 *  2. A set of user-specified columns, all of which are UnitList[_]
 *
 * Derivation:
 *  For every row with a list <a1, a2, list=[i1, i2, i3]>,
 *  creates a new row with identical attributes <a1, a2, i1>, <a1, a2, i2>, etc ...
 */

class ExplodeList(dso: Option[DataSource], columns: Seq[String]) extends Transformer(dso) {

  val isValid = columns.forall(ds.metaSource.metaEntryMap(_).units == UNITS_COMPOSITE_LIST)

  def derive: DataSource = new DataSource {

    // Add column_exploded meta entry for each column
    override lazy val metaSource = ds.metaSource.withMetaEntries(
      columns.map(col => col + "_exploded" -> {
        val originalMetaEntry = ds.metaSource.metaEntryMap(col)
        originalMetaEntry.copy(units = originalMetaEntry.units.unitsChildren.head)
      }).toMap)
      .withoutColumns(columns)

    override lazy val rdd: RDD[DataRow] = {

      // Derivation function for flatMap returns a sequence of DataRows
      def derivation(row: DataRow, cols: Seq[String]): Seq[DataRow] = {

        // Get lists to explode
        val explodedValues = cols.map(col => (col, row(col)))
          .map {
            case (k, ul: UnitsList[_]) => ul.value.map(u => (k + "_exploded", u.asInstanceOf[Units[_]]))
            case (k, v) => throw new RuntimeException(s"Runtime type mismatch: \nexpected: UnitList[_]\nvalue: $v")
          }

        // For multiple expansion columns, explode into the cartesian product
        val combinations = cartesianProduct(explodedValues)

        // For each combination of exploded values, add a row
        for (combination <- combinations) yield {
          row.filterNot(kv => columns.contains(kv._1)) ++ Map(combination: _*)
        }
      }

      // Create the derived dataset
      ds.rdd.flatMap(row => derivation(row, columns))
    }
  }
}
