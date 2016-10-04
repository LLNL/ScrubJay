package scrubjay.units

import scrubjay.meta._

import scala.reflect._

import com.github.nscala_time.time.Imports._

case class DateTimeSpan(v: Interval) extends Units(v) {
  def explode(step: Period): Seq[DateTimeStamp] = {
    Iterator.iterate(v.start)(_.plus(step)).takeWhile(!_.isAfter(v.end)).map(DateTimeStamp(_)).toSeq
  }
}

object DateTimeSpan extends UnitsTag[DateTimeSpan] {
  override val rawValueClass = classTag[Interval]
  override def convert(value: Any, metaUnits: MetaUnits): DateTimeSpan = value match {
    case (s: String, e: String) => DateTimeSpan(DateTime.parse(s) to DateTime.parse(e))
    case v => throw new RuntimeException(s"Cannot convert $v to $metaUnits")
  }
}

