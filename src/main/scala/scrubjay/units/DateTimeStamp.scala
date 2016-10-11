package scrubjay.units

import scrubjay.meta.MetaDescriptor._

import scala.reflect._

import com.github.nscala_time.time.Imports._

case class DateTimeStamp(value: DateTime) extends Units[DateTime]

object DateTimeStamp extends UnitsTag[DateTimeStamp] {
  override val rawValueClassTag = classTag[Int]
  override def convert(value: Any, metaUnits: MetaUnits): DateTimeStamp = value match {
    case s: String => DateTimeStamp(DateTime.parse(s))
    case v => throw new RuntimeException(s"Cannot convert $v to $metaUnits")
  }
}
