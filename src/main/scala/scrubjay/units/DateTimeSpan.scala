package scrubjay.units

import scrubjay.meta._

import com.github.nscala_time.time.Imports._

case class DateTimeSpan(v: Interval) extends Units

object DateTimeSpan {
  val converter = new UnitsConverter[DateTimeSpan] {
    override def convert(value: Any, metaUnits: MetaDescriptor): Units = value match {
      case (s: String, e: String) => DateTimeSpan(DateTime.parse(s) to DateTime.parse(e))
      case v => throw new RuntimeException(s"No known converter from $v to DateTimeSpan")
    }
  }
}

