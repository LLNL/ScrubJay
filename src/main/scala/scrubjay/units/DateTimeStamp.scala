package scrubjay.units

import scrubjay.metabase.MetaDescriptor._
import scrubjay.units.UnitsTag.DomainType
import scrubjay.units.UnitsTag.DomainType.DomainType

import breeze.interpolation.LinearInterpolator
import breeze.linalg.DenseVector
import com.github.nscala_time.time.Imports._

case class DateTimeStamp(value: DateTime) extends Units[DateTime] with Continuous {
  override def asDouble: Double = value.getMillis.toDouble
}

object DateTimeStamp extends UnitsTag[DateTimeStamp, DateTime] {

  override val domainType: DomainType = DomainType.POINT

  override def convert(value: Any, metaUnits: MetaUnits): DateTimeStamp = value match {
    case s: String => DateTimeStamp(DateTime.parse(s))
    case v => throw new RuntimeException(s"Cannot convert $v to $metaUnits")
  }

  override def createInterpolator(xs: Seq[Double], ys: Seq[DateTimeStamp]): (Double) => DateTimeStamp = {
    val f = LinearInterpolator(DenseVector(xs:_*), DenseVector(ys.map(_.value.getMillis.toDouble):_*))
    (d: Double) => DateTimeStamp(Math.round(f(d)).toDateTime)
  }
}
