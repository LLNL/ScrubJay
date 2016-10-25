package scrubjay.units

import scrubjay.metabase.MetaDescriptor._
import scrubjay.units.UnitsTag.DomainType
import scrubjay.units.UnitsTag.DomainType.DomainType

import com.github.nscala_time.time.Imports._

case class DateTimeSpan(value: Interval) extends Units[Interval] with Range {

  override def minDouble: Double = value.getStart.getMillis
  override def maxDouble: Double = value.getEnd.getMillis

  def explode(step: Period): Seq[DateTimeStamp] = {
    Iterator.iterate(value.start)(_.plus(step)).takeWhile(!_.isAfter(value.end)).map(DateTimeStamp(_)).toSeq
  }
}

object DateTimeSpan extends UnitsTag[DateTimeSpan, Interval] {

  override val domainType: DomainType = DomainType.RANGE

  override def convert(value: Any, metaUnits: MetaUnits): DateTimeSpan = value match {
    case (s: String, e: String) => DateTimeSpan(DateTime.parse(s) to DateTime.parse(e))
    case v => throw new RuntimeException(s"Cannot convert $v to $metaUnits")
  }

  override protected def createTypedInterpolator(xs: Seq[Double], ys: Seq[DateTimeSpan]): (Double) => DateTimeSpan = {
    (d: Double) => xs.zip(ys).minBy{case (x, y) => Math.abs(x - d)}._2
  }

  override protected def typedReduce(ys: Seq[DateTimeSpan]): DateTimeSpan = {
    DateTimeSpan(ys.map(_.value.getStart).min to ys.map(_.value.getEnd).max)
  }

}

