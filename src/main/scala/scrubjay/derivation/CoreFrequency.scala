package scrubjay.derivation

import scrubjay.datasource._
import scrubjay.metabase.MetaEntry
import scrubjay.metabase.GlobalMetaBase._
import scrubjay.units._
import org.apache.spark.rdd.RDD

import scrubjay.metabase.MetaDescriptor.MetaRelationType

class CoreFrequency(dso: Option[ScrubJayRDD]) extends Transformer(dso) {

  // Find aperf and mperf time entries
  private val aperfEntry = ds.metaSource.metaEntryMap.find(me =>
      me._2.dimension == DIMENSION_APERF)
  private val mperfEntry = ds.metaSource.metaEntryMap.find(me =>
      me._2.dimension == DIMENSION_MPERF)
  private val baseFreqEntry = ds.metaSource.metaEntryMap.find(me =>
      me._2.dimension == DIMENSION_CPU_BASE_FREQUENCY)

  // Helper functions
  private def addFreqToRow(aperfColumn: String, mperfColumn: String, baseFreqColumn: String, row: DataRow): DataRow = {
    (row(aperfColumn), row(mperfColumn), row(baseFreqColumn)) match {
      case (a: OrderedDiscrete, m: OrderedDiscrete, b: OrderedContinuous) =>
        val newDataRow: DataRow = Map("cpu frequency" -> OrderedContinuous(b.value * (a.value.toDouble / m.value.toDouble)))
        row.filterNot(kv => Set(aperfColumn, mperfColumn).contains(kv._1)) ++ newDataRow
    }
  }

  private def spanFromStartEnd(aperf: Option[String], mperf: Option[String], bfreq: Option[String]): Option[DataRow => DataRow] = {
    (aperf, mperf, bfreq) match {
      case (Some(s), Some(e), Some(b)) => Some((r: DataRow) => addFreqToRow(s, e, b, r))
      case _ => None
    }
  }

  // Create a sequence of possible functions that create a row with a time span from an existing row
  private val allSpans: Seq[Option[(DataRow) => DataRow]] = Seq(spanFromStartEnd(aperfEntry.map(_._1), mperfEntry.map(_._1), baseFreqEntry.map(_._1)))

  override val isValid: Boolean = allSpans.exists(_.isDefined)

  override def derive: ScrubJayRDD = {

    val metaSource = ds.metaSource
      .withMetaEntries(Map("cpu frequency" -> MetaEntry(MetaRelationType.VALUE, MEANING_UNKNOWN, DIMENSION_CPU_ACTIVE_FREQUENCY, UNITS_ORDERED_CONTINUOUS)))
      .withoutColumns(Seq(aperfEntry.get._1, mperfEntry.get._1, baseFreqEntry.get._1))

    val rdd: RDD[DataRow] = {
      ds.map(allSpans.find(_.isDefined).get.get)
    }

    new ScrubJayRDD(rdd, metaSource)
  }
}
