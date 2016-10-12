package scrubjay.derivation

import scrubjay.datasource._
import scrubjay.meta.MetaEntry
import scrubjay.meta.GlobalMetaBase._
import scrubjay.units._
import com.github.nscala_time.time.Imports._
import org.apache.spark.rdd.RDD

object DeriveTimeSpan {

  def deriveTimeSpan(ds: DataSource): Option[DataSource] = {

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

    val defined = allSpans.exists(_.isDefined)

    if (!defined) {
      None
    }
    else {
      Some(
        new DataSource{
          val metaSource = ds.metaSource.withMetaEntries(Map("span" -> MetaEntry(MEANING_SPAN, DIMENSION_TIME, UNITS_DATETIMESPAN)))
          lazy val rdd: RDD[DataRow] = {
            ds.rdd.map(allSpans.find(_.isDefined).get.get)
          }
        }
      )
    }
  }
}
