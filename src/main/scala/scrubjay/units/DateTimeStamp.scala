package scrubjay.units

import scrubjay.metabase.MetaDescriptor._
import scrubjay.units.UnitsTag.DomainType
import scrubjay.units.UnitsTag.DomainType.DomainType

import breeze.interpolation.LinearInterpolator
import breeze.linalg.DenseVector
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat

import scala.util.control.Exception.allCatch

case class DateTimeStamp(value: Double) extends Units[Double] with Continuous {
  override def asDouble: Double = value
  override def toString: String = "DateTimeStamp(" + rawString + ")"
  override def rawString: String = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss.SSSZ").print(new DateTime(value.toLong))
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

    DateTimeStamp(attempts.head.getMillis)
  }

  // FIXME: remove null
  override def convert(value: Any, metaUnits: MetaUnits = null): DateTimeStamp = value match {
    case dt: DateTime => DateTimeStamp(dt.getMillis)
    case d: Double => DateTimeStamp(d*1000L)
    case f: Float => DateTimeStamp(f*1000L)
    case l: Long => DateTimeStamp(l*1000L)
    case i: Int => DateTimeStamp(i*1000L)
    case v => dateTimeFromString(v.toString)
  }

  override protected def createTypedInterpolator(xs: Seq[Double], ys: Seq[DateTimeStamp]): (Double) => DateTimeStamp = {
    (d: Double) => DateTimeStamp(d)
  }

  override protected def typedReduce(ys: Seq[DateTimeStamp]): DateTimeStamp = {
    DateTimeStamp(ys.map(_.value).sum / ys.length)
  }
}
