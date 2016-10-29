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
  override def rawString: String = "'" + value.toString + "'"
}

object DateTimeStamp extends UnitsTag[DateTimeStamp, DateTime] {

  override val domainType: DomainType = DomainType.POINT

  def dateTimeFromString(s: String): DateTimeStamp = {

    val attempts = Seq(
      allCatch.opt(DateTime.parse(s)),
      allCatch.opt(DateTimeFormat.forPattern("E MMM d H:m:s z y").parseDateTime(s))
    ).flatten

    if (attempts.isEmpty)
      throw new RuntimeException(s"Cannot convert $s to DateTime")

    DateTimeStamp(attempts.head)
  }

  override def convert(value: Any, metaUnits: MetaUnits): DateTimeStamp = value match {
    case dt: DateTime => DateTimeStamp(dt)
    case d: Double => DateTimeStamp((Math.round(d)*1000L).toDateTime)
    case f: Float => DateTimeStamp((Math.round(f)*1000L).toDateTime)
    case l: Long => DateTimeStamp((l*1000L).toDateTime)
    case i: Int => DateTimeStamp((i*1000L).toDateTime)
    case v => dateTimeFromString(v.toString)
  }

  override protected def createTypedInterpolator(xs: Seq[Double], ys: Seq[DateTimeStamp]): (Double) => DateTimeStamp = {
    val f = LinearInterpolator(DenseVector(xs:_*), DenseVector(ys.map(_.value.getMillis.toDouble):_*))
    (d: Double) => DateTimeStamp((Math.round(d)*1000L).toDateTime)
  }

  override protected def typedReduce(ys: Seq[DateTimeStamp]): DateTimeStamp = {
    DateTimeStamp(Math.round(ys.map(_.value.getMillis.toDouble).sum / ys.length).toDateTime)
  }
}
