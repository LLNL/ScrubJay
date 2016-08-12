package scrubjay.derivation

import scrubjay._
import scrubjay.datasource._
import scrubjay.meta._
import scrubjay.meta.GlobalMetaBase._
import scrubjay.units._

import com.github.nscala_time.time.Imports._

class DeriveTimeSpan(metaOntology: MetaBase,
                     ds: DataSource) {

  def spanFromStartEnd(start: Option[String], end: Option[String]): Option[DataRow => DateTimeSpan] = {
    if (start.isDefined && end.isDefined)
      Some((r: DataRow) => (r(start.get), r(end.get)) match {
        case (s: DateTimeStamp, e: DateTimeStamp) => DateTimeSpan(s.v to e.v)
      })
    else
      None
  }

  val allSpans = Seq(
    spanFromStartEnd(
      ds.metaEntryMap.find(_._2.meaning == MEANING_START).map(_._1),
      ds.metaEntryMap.find(_._2.meaning == MEANING_END).map(_._1))
  )

  val defined = allSpans.exists(_.isDefined)

}

object DeriveTimeSpan {
  implicit class ScrubJaySession_DeriveTimeSpan(sjs: ScrubJaySession) {
    def deriveTimeSpan(ds: DataSource): DeriveTimeSpan = {
      new DeriveTimeSpan(sjs.metaOntology, ds)
    }
  }
}