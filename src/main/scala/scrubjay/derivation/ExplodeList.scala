package scrubjay.derivation

import scrubjay._
import scrubjay.datasource._
import scrubjay.meta._
import scrubjay.meta.GlobalMetaBase._
import scrubjay.units._

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

class ExplodeList(ds: DataSource, columns: Seq[String]) extends Derivation(ds) {

  val isValid = columns.map(ds.metaSource.metaEntryMap(_)).forall(_.units.unitsTag == UnitsList)

  def derive: DataSource = new DataSource(null, null, null) {

    // Add column_exploded meta entry for each column
    override lazy val metaSource = ds.metaSource.withMetaEntries(
      columns.map(col => col + "_exploded" -> {
        val originalMetaEntry = ds.metaSource.metaEntryMap(col)
        originalMetaEntry.copy(units = originalMetaEntry.units.unitsChildren.head)
      }).toMap)

    override lazy val rdd: RDD[DataRow] = {

      // For multiple expansion columns, explode into the cartesian product
      def cartesianProduct[T](xss: List[List[T]]): List[List[T]] = xss match {
        case Nil => List(Nil)
        case h :: t => for (xh <- h; xt <- cartesianProduct(t)) yield xh :: xt
      }

      // Derivation function for flatMap returns a sequence of DataRows
      def derivation(row: DataRow, cols: Seq[String]): Seq[DataRow] = {

        // Get lists to explode
        val explodedValues =
          row.filter { case (k, v) => cols.contains(k) }
            .map {
              case (k, ul: UnitsList[_]) => ul.value.map { case u: Units[_] => (k + "_exploded", u) }
              case (k, v) => throw new RuntimeException(s"Runtime type mismatch: \nexpected: UnitList[_]\nvalue: $v")
            }
            .toList

        val combinations = cartesianProduct(explodedValues)

        // For each combination of exploded values, add a row
        for (combination <- combinations) yield {
          row ++ Map(combination: _*)
        }
      }

      // Create the derived dataset
      ds.rdd.flatMap(row =>
        derivation(row, columns))
    }
  }
}
