package scrubjay.units

import scrubjay.meta._

import com.github.nscala_time.time.Imports._

case class DateTimeStamp(v: DateTime) extends Units {
  val raw = v
}

object DateTimeStamp {
  val converter = new UnitsConverter[DateTimeStamp] {
    override def convert(value: Any, metaUnits: MetaDescriptor): Units = value match {
      case s: String => DateTimeStamp(DateTime.parse(s))
      case v => throw new RuntimeException(s"No known converter from $v to DateTimeStamp")
    }
  }
}
