package scrubjay.derivation

import org.apache.spark.rdd.RDD
import scrubjay.MetaEntry
import scrubjay.datasource.{DataRow, DataSourceID, ScrubJayRDD}
import scrubjay.metasource._

class MergeColumns(dsID: DataSourceID, columns: Seq[String]) extends DataSourceID(Seq(dsID))(Seq(columns)) {

  def newColumn: String = columns.mkString("_")
  def metaEntry: MetaEntry = dsID.metaSource(columns.head)

  def isValid: Boolean = columns.nonEmpty &&
    columns.forall(dsID.metaSource.columns contains _) &&
    columns.forall(c => dsID.metaSource(c).units == dsID.metaSource(columns.head).units)

  val metaSource: MetaSource = dsID.metaSource.withMetaEntries(Map(newColumn -> metaEntry))
    .withoutColumns(columns)

  def realize: ScrubJayRDD = {

    val ds = dsID.realize

    val rdd: RDD[DataRow] = {

      val reducer = metaEntry.units.unitsTag.reduce _

      ds.map(row => {
        val mergedVal = reducer(columns.map(row))
        row.filterNot{case (k, _) => columns.contains(k)} ++ Map(newColumn -> mergedVal)
      })
    }

    new ScrubJayRDD(rdd)
  }
}

object MergeColumns {
  def apply(dsID: DataSourceID, columns: Seq[String]): Option[DataSourceID] = {
    val derivedID = new MergeColumns(dsID, columns)
    if(derivedID.isValid)
      Some(derivedID)
    else
      None
  }
}
