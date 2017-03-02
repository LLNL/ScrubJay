package scrubjay.derivation

import org.apache.spark.rdd.RDD
import scrubjay.datasource.{DataRow, ScrubJayRDD}
import scrubjay.metasource._

class MergeColumns(dso: Option[ScrubJayRDD], columns: Seq[String]) extends Transformer(dso) {

  private val newColumn = columns.mkString("_")
  private val metaEntry = ds.metaSource(columns.head)

  override val isValid: Boolean = columns.nonEmpty &&
    columns.forall(ds.metaSource.columns contains _) &&
    columns.forall(c => ds.metaSource(c).units == ds.metaSource(columns.head).units)

  override def derive: ScrubJayRDD = {

    val metaSource = ds.metaSource.withMetaEntries(Map(newColumn -> metaEntry))
      .withoutColumns(columns)

    val rdd: RDD[DataRow] = {

      val reducer = metaEntry.units.unitsTag.reduce _

      ds.map(row => {
        val mergedVal = reducer(columns.map(row))
        row.filterNot{case (k, _) => columns.contains(k)} ++ Map(newColumn -> mergedVal)
      })
    }

    new ScrubJayRDD(rdd, metaSource)
  }
}
