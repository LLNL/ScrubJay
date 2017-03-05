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
 *  2. A set of user-specified columns, all of which are explode-able rows
 *
 * Derivation:
 *  For every row with an explode-able element,
 *  explodes the row, duplicating all other columns
 */

case class ExplodeDiscreteRange(dsID: DataSourceID, column: String)
  extends DataSourceID {

  val newColumn: String = column + "_exploded"

  // Add column_exploded meta entry for each column
  val metaSource: MetaSource = {
    if (isValid) {
      val originalMetaEntry = dsID.metaSource(column)
      val newMetaEntry = originalMetaEntry.copy(units = originalMetaEntry.units.unitsChildren.head)
      dsID.metaSource.withMetaEntries(Map(newColumn -> newMetaEntry)).withoutColumns(Seq(column))
    }
    else
      dsID.metaSource
  }

  def isValid: Boolean = dsID.metaSource(column).units == UNITS_COMPOSITE_LIST

  def realize: ScrubJayRDD = {

    val ds = dsID.realize

    val rdd: RDD[DataRow] = {

      // Derivation function for flatMap returns a sequence of DataRows
      def derivation(row: DataRow, col: String): Seq[DataRow] = {

        // Get lists to explode
        val valueToExplode = row(col)
        val explodedValues: Iterator[Units[_]] = valueToExplode.asInstanceOf[DiscreteRange].explode
        val rowWithoutColumn: Map[String, Units[_]] = row.filterNot(kv => column == kv._1)

        // For each combination of exploded values, add a row
        val newRows = for (elem <- explodedValues) yield {
           rowWithoutColumn ++ Map(newColumn -> elem)
        }

        newRows.toSeq
      }

      // Create the derived dataset
      ds.flatMap(row => derivation(row, column))
    }

    new ScrubJayRDD(rdd)
  }
}
