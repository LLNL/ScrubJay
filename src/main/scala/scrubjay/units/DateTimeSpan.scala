package scrubjay.units

import scrubjay.metabase.MetaDescriptor._

import scala.reflect._
import com.github.nscala_time.time.Imports._
import scrubjay.units.UnitsTag.DomainType
import scrubjay.units.UnitsTag.DomainType.DomainType

case class DateTimeSpan(value: Interval) extends Units[ Interval] {
  def explode(step: Period): Seq[DateTimeStamp] = {
    Iterator.iterate(value.start)(_.plus(step)).takeWhile(!_.isAfter(value.end)).map(DateTimeStamp(_)).toSeq
  }
}

object DateTimeSpan extends UnitsTag[DateTimeSpan] {

  override val rawValueClassTag = classTag[Interval]
  override val domainType: DomainType = DomainType.RANGE

  override def convert(value: Any, metaUnits: MetaUnits): DateTimeSpan = value match {
    case (s: String, e: String) => DateTimeSpan(DateTime.parse(s) to DateTime.parse(e))
    case v => throw new RuntimeException(s"Cannot convert $v to $metaUnits")
  }

  override def createInterpolator(xs: Seq[Double], ys: Seq[DateTimeSpan]): (Double) => DateTimeSpan = {
    (d: Double) => xs.zip(ys).minBy{case (x, y) => Math.abs(x - d)}._2
  }

}

