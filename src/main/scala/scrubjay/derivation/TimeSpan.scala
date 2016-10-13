package scrubjay.derivation

import scrubjay.datasource._
import scrubjay.metabase.MetaEntry
import scrubjay.metabase.GlobalMetaBase._
import scrubjay.units._
import com.github.nscala_time.time.Imports._
import org.apache.spark.rdd.RDD

class TimeSpan(dso: Option[DataSource]) extends Transformer(dso) {

  // Helper functions
  def addSpanToRow(startColumn: String, endColumn: String, row: DataRow): DataRow = {
    (row(startColumn), row(endColumn)) match {
      case (s: DateTimeStamp, e: DateTimeStamp) => row ++ Map("span" -> DateTimeSpan(s.value to e.value))
    }
  }

  def spanFromStartEnd(start: Option[String], end: Option[String]): Option[DataRow => DataRow] = {
    (start, end) match {
      case (Some(s), Some(e)) => Some((r: DataRow) => addSpanToRow(s, e, r))
      case _ => None
    }
  }

  // Create a sequence of possible functions that create a row with a time span from an existing row
  val allSpans: Seq[Option[(DataRow) => DataRow]] = Seq(
    spanFromStartEnd(
      ds.metaSource.metaEntryMap.find(_._2.meaning == MEANING_START).map(_._1),
      ds.metaSource.metaEntryMap.find(_._2.meaning == MEANING_END).map(_._1))
  )

  val isValid = allSpans.exists(_.isDefined)

  def derive = new DataSource {
    override lazy val metaSource = ds.metaSource.withMetaEntries(Map("span" -> MetaEntry(MEANING_SPAN, DIMENSION_TIME, UNITS_DATETIMESPAN)))
    override lazy val rdd: RDD[DataRow] = {
      ds.rdd.map(allSpans.find(_.isDefined).get.get)
    }
  }
}
