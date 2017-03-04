package scrubjay.derivation

import scrubjay.datasource._
import scrubjay.metasource._
import scrubjay.metabase.MetaEntry
import scrubjay.metabase.GlobalMetaBase._
import scrubjay.units._
import scrubjay.metabase._
import scrubjay.metabase.MetaDescriptor.MetaRelationType

import org.apache.spark.rdd.RDD

case class TimeSpan(dsID: DataSourceID)
  extends DataSourceID {

  // Find start and end time entries
  def startEntry: Option[(String, MetaEntry)] = dsID.metaSource.find(me =>
    me._2.relationType == MetaRelationType.DOMAIN &&
      me._2.dimension == DIMENSION_TIME &&
      me._2.meaning == MEANING_START)
  def endEntry: Option[(String, MetaEntry)] = dsID.metaSource.find(me =>
    me._2.relationType == MetaRelationType.DOMAIN &&
      me._2.dimension == DIMENSION_TIME &&
      me._2.meaning == MEANING_END)

  // Helper functions
  def addSpanToRow(startColumn: String, endColumn: String, row: DataRow): DataRow = {
    (row(startColumn), row(endColumn)) match {
      case (s: DateTimeStamp, e: DateTimeStamp) =>
        val newDataRow: DataRow = Map("span" -> DateTimeSpan((s.value, e.value)))
        row.filterNot(kv => Set(startColumn, endColumn).contains(kv._1)) ++ newDataRow
    }
  }

  def spanFromStartEnd(start: Option[String], end: Option[String]): Option[DataRow => DataRow] = {
    (start, end) match {
      case (Some(s), Some(e)) => Some((r: DataRow) => addSpanToRow(s, e, r))
      case _ => None
    }
  }

  // Create a sequence of possible functions that create a row with a time span from an existing row
  def allSpans: Seq[Option[(DataRow) => DataRow]] = Seq(spanFromStartEnd(startEntry.map(_._1), endEntry.map(_._1)))

  def isValid: Boolean = allSpans.exists(_.isDefined)

  val metaSource: MetaSource = dsID.metaSource
    .withMetaEntries(Map("span" -> MetaEntry(MetaRelationType.DOMAIN, MEANING_SPAN, DIMENSION_TIME, UNITS_DATETIMESPAN)))
    .withoutColumns(Seq(startEntry.get._1, endEntry.get._1))

  def realize: ScrubJayRDD = {

    val ds = dsID.realize

    val rdd: RDD[DataRow] = {
      ds.map(allSpans.find(_.isDefined).get.get)
    }

    new ScrubJayRDD(rdd)
  }
}
