package scrubjay.derivation

import scrubjay.datasource._
import scrubjay.metabase.GlobalMetaBase._
import scrubjay.units._
import scrubjay.metasource._

import org.apache.spark.rdd.RDD

case class ExplodeContinuousRange(dsID: DataSourceID, column: String, period: Double)
  extends DataSourceID {

  val newColumn: String = column + "_exploded"

  // Add column_exploded meta entry for each column
  val metaSource: MetaSource = {
    val originalMetaEntry = dsID.metaSource(column)
    val newMeta = originalMetaEntry.copy(units = UNITS_DATETIMESTAMP)
    dsID.metaSource
      .withoutColumns(Seq(column))
      .withMetaEntries(Map(newColumn -> newMeta))
  }

  def isValid: Boolean = dsID.metaSource(column).units == UNITS_DATETIMESPAN

  def realize: ScrubJayRDD = {

    val ds = dsID.realize

    val rdd: RDD[DataRow] = {

      // Derivation function for flatMap returns a sequence of DataRows
      def derivation(row: DataRow, col: String): Seq[DataRow] = {

        // Get lists to explode
        val valueToExplode = row(col)
        val explodedValues: Iterator[Units[_]] = valueToExplode.asInstanceOf[ContinuousRange[Double]].explode(period)
        val rowWithoutColumn: Map[String, Units[_]] = row.filterNot(kv => column == kv._1)

        explodedValues.map(newValue => rowWithoutColumn ++ Map(newColumn -> newValue)).toSeq
      }

      // Create the derived dataset
      ds.flatMap(row => derivation(row, column))
    }

    new ScrubJayRDD(rdd)
  }
}
