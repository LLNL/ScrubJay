package scrubjay.derivation

import scrubjay.datasource._
import scrubjay.metabase.MetaEntry
import scrubjay.metabase.GlobalMetaBase._
import scrubjay.metasource._
import scrubjay.units._
import org.apache.spark.rdd.RDD
import scrubjay.metabase._
import scrubjay.metabase.MetaDescriptor.MetaRelationType

case class CoreFrequency(dsID: DataSourceID)
  extends DataSourceID {

  // Find aperf and mperf time entries
  def aperfEntry: Option[(String, MetaEntry)] = dsID.metaSource.find(me =>
      me._2.dimension == DIMENSION_APERF)
  def mperfEntry: Option[(String, MetaEntry)] = dsID.metaSource.find(me =>
      me._2.dimension == DIMENSION_MPERF)
  def baseFreqEntry: Option[(String, MetaEntry)] = dsID.metaSource.find(me =>
      me._2.dimension == DIMENSION_CPU_BASE_FREQUENCY)

  // Helper functions
  def addFreqToRow(aperfColumn: String, mperfColumn: String, baseFreqColumn: String, row: DataRow): DataRow = {
    if (Seq(aperfColumn, mperfColumn, baseFreqColumn).forall(row.keys.toList.contains)) {
        (row(aperfColumn), row(mperfColumn), row(baseFreqColumn)) match {
          case (a: Accumulation, m: Accumulation, b: OrderedContinuous) =>
            val newDataRow: DataRow = Map("cpu frequency" -> OrderedContinuous(b.value * (a.value.toDouble / m.value.toDouble)))
            row.filterNot(kv => Set(aperfColumn, mperfColumn).contains(kv._1)) ++ newDataRow
        }
    } else {
        row
    }
  }

  def spanFromStartEnd(aperf: Option[String], mperf: Option[String], bfreq: Option[String]): Option[DataRow => DataRow] = {
    (aperf, mperf, bfreq) match {
      case (Some(s), Some(e), Some(b)) => Some((r: DataRow) => addFreqToRow(s, e, b, r))
      case _ => None
    }
  }

  // Create a sequence of possible functions that create a row with a time span from an existing row
  def allSpans: Seq[Option[(DataRow) => DataRow]] = Seq(spanFromStartEnd(aperfEntry.map(_._1), mperfEntry.map(_._1), baseFreqEntry.map(_._1)))

  val isValid: Boolean = allSpans.exists(_.isDefined)

  val metaSource: MetaSource = dsID.metaSource
    .withMetaEntries(Map("cpu frequency" -> MetaEntry(MetaRelationType.VALUE, DIMENSION_CPU_ACTIVE_FREQUENCY, UNITS_ORDERED_CONTINUOUS)))
    .withoutColumns(Seq(aperfEntry.get._1, mperfEntry.get._1, baseFreqEntry.get._1))

  def realize: ScrubJayRDD = {

    val ds = dsID.realize

    val rdd: RDD[DataRow] = {
      ds.map(allSpans.find(_.isDefined).get.get)
    }

    new ScrubJayRDD(rdd)
  }
}
