package scrubjay.derivation

import scrubjay._
import scrubjay.datasource._
import scrubjay.meta._
import scrubjay.meta.MetaEntry
import scrubjay.meta.GlobalMetaBase._
import scrubjay.units._
import com.github.nscala_time.time.Imports._
import org.apache.spark.rdd.RDD

class DeriveTimeSpan(metaOntology: MetaBase,
                     ds: DataSource) extends DerivedDataSource(metaOntology) {

  def addSpanToRow(startColumn: String, endColumn: String)(row: DataRow): DataRow = {
    (row(startColumn), row(endColumn)) match {
      case (s: DateTimeStamp, e: DateTimeStamp) => row ++ Map("span" -> DateTimeSpan(s.v to e.v))
    }
  }

  def spanFromStartEnd(start: Option[String], end: Option[String]): Option[DataRow => DataRow] = {
    (start, end) match {
      case (Some(s), Some(e)) => Some(addSpanToRow(s, e)(_))
      case _ => None
    }
  }

  val allSpans = Seq(
    spanFromStartEnd(
      ds.metaEntryMap.find(_._2.meaning == MEANING_START).map(_._1),
      ds.metaEntryMap.find(_._2.meaning == MEANING_END).map(_._1))
  )

  override val defined = allSpans.exists(_.isDefined)

  override val metaEntryMap: MetaMap = ds.metaEntryMap ++ Map("span" -> MetaEntry(MEANING_SPAN, DIMENSION_TIME, UNITS_DATETIMESPAN))

  lazy val rdd: RDD[DataRow] = {
    ds.rdd.map(allSpans.find(_.isDefined).get.get)
  }
}

object DeriveTimeSpan {
  implicit class ScrubJaySession_DeriveTimeSpan(sjs: ScrubJaySession) {
    def deriveTimeSpan(ds: DataSource): DeriveTimeSpan = {
      new DeriveTimeSpan(sjs.metaOntology, ds)
    }
  }
}
