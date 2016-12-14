package scrubjay.units

import scrubjay.metabase.MetaDescriptor._
import scrubjay.units.UnitsTag.DomainType
import scrubjay.units.UnitsTag.DomainType.DomainType

import breeze.interpolation.LinearInterpolator
import breeze.linalg.DenseVector
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat

import scala.util.control.Exception.allCatch

case class DateTimeStamp(value: DateTime) extends Units[DateTime] with Continuous {
  override def asDouble: Double = value.getMillis.toDouble
  override def rawString: String = "'" + DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss.SSSZ").print(value) + "'"
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
    case d: Double => DateTimeStamp(new DateTime(Math.round(d*1000)))
    case f: Float => DateTimeStamp(new DateTime(Math.round(f*1000).toLong))
    case l: Long => DateTimeStamp(new DateTime(l*1000L))
    case i: Int => DateTimeStamp(new DateTime(i*1000L))
    case v => dateTimeFromString(v.toString)
  }

  override protected def createTypedInterpolator(xs: Seq[Double], ys: Seq[DateTimeStamp]): (Double) => DateTimeStamp = {
    val f = LinearInterpolator(DenseVector(xs:_*), DenseVector(ys.map(_.value.getMillis.toDouble):_*))
    (d: Double) => DateTimeStamp(new DateTime(Math.round(d)*1000L))
  }

  override protected def typedReduce(ys: Seq[DateTimeStamp]): DateTimeStamp = {
    DateTimeStamp(new DateTime(Math.round(ys.map(_.value.getMillis.toDouble).sum / ys.length)))
  }
}
