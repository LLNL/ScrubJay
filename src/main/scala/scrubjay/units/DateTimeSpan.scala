package scrubjay.units

import org.joda.time.format.DateTimeFormat
import scrubjay.metabase.MetaDescriptor._
import scrubjay.units.UnitsTag.DomainType
import scrubjay.units.UnitsTag.DomainType.DomainType
import org.joda.time.{DateTime, Interval, Period}

case class DateTimeSpan(value: (Double, Double)) extends Units[(Double, Double)] with Range {

  def getStart: Double = value._1
  def getEnd: Double = value._2

  override def minDouble: Double = getStart
  override def maxDouble: Double = getEnd

  // TODO: read in this rawstring format
  override def rawString: String = "('" +
    DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss.SSSZ").print(getStart.toLong) +
    DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss.SSSZ").print(getEnd.toLong) + "')"

  def explode(step: Double): Seq[DateTimeStamp] = {
    Iterator.iterate(getStart)(_ + step).takeWhile(_ < getEnd).map(DateTimeStamp(_)).toSeq
  }
}

object DateTimeSpan extends UnitsTag[DateTimeSpan, (Double, Double)] {

  override val domainType: DomainType = DomainType.RANGE

  override def convert(value: Any, metaUnits: MetaUnits): DateTimeSpan = value match {
    case (s: String, e: String) => DateTimeSpan((DateTime.parse(s).getMillis, DateTime.parse(e).getMillis))
    case v => throw new RuntimeException(s"Cannot convert $v to $metaUnits")
  }

  override protected def createTypedInterpolator(xs: Seq[Double], ys: Seq[DateTimeSpan]): (Double) => DateTimeSpan = {
    (d: Double) => xs.zip(ys).minBy{case (x, _) => Math.abs(x - d)}._2
  }

  override protected def typedReduce(ys: Seq[DateTimeSpan]): DateTimeSpan = {
    DateTimeSpan((ys.map(_.getStart).min, ys.map(_.getEnd).max))
  }

}

