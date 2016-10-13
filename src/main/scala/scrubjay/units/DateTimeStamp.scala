package scrubjay.units

import scrubjay.metabase.MetaDescriptor._

import scala.reflect._
import com.github.nscala_time.time.Imports._
import scrubjay.units.UnitsTag.DomainType
import scrubjay.units.UnitsTag.DomainType.DomainType

case class DateTimeStamp(value: DateTime) extends Units[DateTime]

object DateTimeStamp extends UnitsTag[DateTimeStamp] {
  override val rawValueClassTag = classTag[Int]
  override val domainType: DomainType = DomainType.POINT
  override def convert(value: Any, metaUnits: MetaUnits): DateTimeStamp = value match {
    case s: String => DateTimeStamp(DateTime.parse(s))
    case v => throw new RuntimeException(s"Cannot convert $v to $metaUnits")
  }
}
