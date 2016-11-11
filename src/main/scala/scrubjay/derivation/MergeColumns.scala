package scrubjay.derivation

import org.apache.spark.rdd.RDD
import scrubjay.DataSource
import scrubjay.datasource.DataRow

class MergeColumns(dso: Option[DataSource], columns: Seq[String]) extends Transformer(dso) {

  val newColumn = columns.mkString("_")
  val metaEntry = ds.metaSource.metaEntryMap(columns.head)

  val isValid = columns.nonEmpty &&
    columns.forall(ds.metaSource.columns contains _) &&
    columns.forall(c => ds.metaSource.metaEntryMap(c).units == ds.metaSource.metaEntryMap(columns.head).units)

  def derive: DataSource = new DataSource {

    override lazy val metaSource = ds.metaSource.withMetaEntries(Map(newColumn -> metaEntry))
      .withoutColumns(columns)

    override lazy val rdd: RDD[DataRow] = {

      val reducer = metaEntry.units.unitsTag.reduce _

      ds.rdd.map(row => {
        val mergedVal = reducer(columns.map(row))
        row.filterNot{case (k, v) => columns.contains(k)} ++ Map(newColumn -> mergedVal)
      })
    }
  }
}
