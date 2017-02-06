package scrubjay.derivation

import scrubjay.datasource._
import scrubjay.metabase.MetaEntry
import scrubjay.metabase.GlobalMetaBase._
import scrubjay.units._
import org.apache.spark.rdd.RDD
import org.joda.time.Interval
import scrubjay.metabase.MetaDescriptor.MetaRelationType

class TimeSpan(dso: Option[ScrubJayRDD]) extends Transformer(dso) {

  // Find start and end time entries
  private val startEntry = ds.metaSource.metaEntryMap.find(me =>
    me._2.relationType == MetaRelationType.DOMAIN &&
      me._2.dimension == DIMENSION_TIME &&
      me._2.meaning == MEANING_START)
  private val endEntry = ds.metaSource.metaEntryMap.find(me =>
    me._2.relationType == MetaRelationType.DOMAIN &&
      me._2.dimension == DIMENSION_TIME &&
      me._2.meaning == MEANING_END)

  // Helper functions
  private def addSpanToRow(startColumn: String, endColumn: String, row: DataRow): DataRow = {
    (row(startColumn), row(endColumn)) match {
      case (s: DateTimeStamp, e: DateTimeStamp) =>
        val newDataRow: DataRow = Map("span" -> DateTimeSpan((s.value, e.value)))
        row.filterNot(kv => Set(startColumn, endColumn).contains(kv._1)) ++ newDataRow
    }
  }

  private def spanFromStartEnd(start: Option[String], end: Option[String]): Option[DataRow => DataRow] = {
    (start, end) match {
      case (Some(s), Some(e)) => Some((r: DataRow) => addSpanToRow(s, e, r))
      case _ => None
    }
  }

  // Create a sequence of possible functions that create a row with a time span from an existing row
  private val allSpans: Seq[Option[(DataRow) => DataRow]] = Seq(spanFromStartEnd(startEntry.map(_._1), endEntry.map(_._1)))

  override val isValid: Boolean = allSpans.exists(_.isDefined)

  override def derive: ScrubJayRDD = {

    val metaSource = ds.metaSource
      .withMetaEntries(Map("span" -> MetaEntry(MetaRelationType.DOMAIN, MEANING_SPAN, DIMENSION_TIME, UNITS_DATETIMESPAN)))
      .withoutColumns(Seq(startEntry.get._1, endEntry.get._1))

    val rdd: RDD[DataRow] = {
      ds.map(allSpans.find(_.isDefined).get.get)
    }

    new ScrubJayRDD(rdd, metaSource)
  }
}
