package scrubjay.derivation

import scrubjay.datasource._
import scrubjay.metabase.GlobalMetaBase._
import scrubjay.units._
import scrubjay.util.cartesianProduct
import scrubjay.metasource._

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

class ExplodeList(ds: Option[ScrubJayRDD], columns: Seq[String]) {

  override val isValid: Boolean = columns.forall(ds.metaSource(_).units == UNITS_COMPOSITE_LIST)

  override def derive: ScrubJayRDD = {

    // Add column_exploded meta entry for each column
    val metaSource = ds.metaSource.withMetaEntries(
      columns.map(col => col + "_exploded" -> {
        val originalMetaEntry = ds.metaSource(col)
        originalMetaEntry.copy(units = originalMetaEntry.units.unitsChildren.head)
      }).toMap)
      .withoutColumns(columns)

    val rdd: RDD[DataRow] = {

      // Derivation function for flatMap returns a sequence of DataRows
      def derivation(row: DataRow, cols: Seq[String]): Seq[DataRow] = {

        // Get lists to explode
        val explodedValues = cols.map(col => (col, row(col)))
          .map {
            case (k, ul: UnitsList[_]) => ul.value.map(u => (k + "_exploded", u.asInstanceOf[Units[_]]))
            case (_, v) => throw new RuntimeException(s"Runtime type mismatch: \nexpected: UnitList[_]\nvalue: $v")
          }

        // For multiple expansion columns, explode into the cartesian product
        val combinations = cartesianProduct(explodedValues)

        // For each combination of exploded values, add a row
        for (combination <- combinations) yield {
          row.filterNot(kv => columns.contains(kv._1)) ++ Map(combination: _*)
        }
      }

      // Create the derived dataset
      ds.flatMap(row => derivation(row, columns))
    }

    new ScrubJayRDD(rdd, metaSource)
  }
}
