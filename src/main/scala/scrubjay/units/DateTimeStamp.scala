package scrubjay.units

import scrubjay.metabase.MetaDescriptor._
import scrubjay.units.UnitsTag.DomainType
import scrubjay.units.UnitsTag.DomainType.DomainType
import scrubjay.util.niceAttempt

import breeze.interpolation.LinearInterpolator
import breeze.linalg.DenseVector
import com.github.nscala_time.time.Imports._
import scala.util.control.Exception.allCatch

case class DateTimeStamp(value: DateTime) extends Units[DateTime] with Continuous {
  override def asDouble: Double = value.getMillis.toDouble
}

object DateTimeStamp extends UnitsTag[DateTimeStamp, DateTime] {

  override val domainType: DomainType = DomainType.POINT

  def dateTimeFromString(s: String): DateTimeStamp = {

    val attempts = Seq(
      allCatch.opt(DateTime.parse(s)),
      allCatch.opt(DateTimeFormat.forPattern("E MMM d H:m:s z y").parseDateTime(s))
    )

    DateTimeStamp(attempts.flatten.head)
  }

  override def convert(value: Any, metaUnits: MetaUnits): DateTimeStamp = value match {
    case dt: DateTime => DateTimeStamp(dt)
    case s: String => dateTimeFromString(s)
    case d: Double => DateTimeStamp((Math.round(d)*1000L).toDateTime)
    case f: Float => DateTimeStamp((Math.round(f)*1000L).toDateTime)
    case l: Long => DateTimeStamp((l*1000L).toDateTime)
    case i: Int => DateTimeStamp((i*1000L).toDateTime)
    case v => throw new RuntimeException(s"Cannot convert $v to $metaUnits")
  }

  override protected def createTypedInterpolator(xs: Seq[Double], ys: Seq[DateTimeStamp]): (Double) => DateTimeStamp = {
    val f = LinearInterpolator(DenseVector(xs:_*), DenseVector(ys.map(_.value.getMillis.toDouble):_*))
    (d: Double) => DateTimeStamp((Math.round(d)*1000L).toDateTime)
  }

  override protected def typedReduce(ys: Seq[DateTimeStamp]): DateTimeStamp = {
    DateTimeStamp(Math.round(ys.map(_.value.getMillis.toDouble).sum / ys.length).toDateTime)
  }
}
