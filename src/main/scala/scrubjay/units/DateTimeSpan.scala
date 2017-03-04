package scrubjay.units

import org.joda.time.format.DateTimeFormat
import scrubjay.metabase.MetaDescriptor._
import scrubjay.units.UnitsTag.DomainType
import scrubjay.units.UnitsTag.DomainType.DomainType
import org.joda.time.{DateTime, Interval, Period}

case class DateTimeSpan(value: (DateTimeStamp, DateTimeStamp)) extends Units[(DateTimeStamp, DateTimeStamp)] with Range[DateTimeStamp, Double] {

  override def min: DateTimeStamp = value._1
  override def max: DateTimeStamp = value._2

  override def rawString: String = "('" +
    DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss.SSSZ").print(min.asDouble.toLong) +
    DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss.SSSZ").print(max.asDouble.toLong) + "')"

  override def explode(period: Double): Iterator[DateTimeStamp] = {
    Iterator.iterate(min)(current => DateTimeStamp(current.asDouble + period)).takeWhile(_.asDouble < max.asDouble)
  }
}

object DateTimeSpan extends UnitsTag[DateTimeSpan, (DateTimeStamp, DateTimeStamp)] {

  override val domainType: DomainType = DomainType.RANGE

  // FIXME: remove nulls!
  override def convert(value: Any, metaUnits: MetaUnits = null): DateTimeSpan = value match {
    case s: String => {
      val startEnd = s.stripPrefix("(").stripSuffix(")").split(",")
      DateTimeSpan((DateTimeStamp.convert(startEnd(0)), DateTimeStamp.convert(startEnd(1))))
    }
    case (start: String, end: String) => {
      DateTimeSpan((DateTimeStamp.convert(start), DateTimeStamp.convert(end)))
    }
    case v => throw new RuntimeException(s"Cannot convert $v to $metaUnits")
  }

  override protected def createTypedInterpolator(xs: Seq[Double], ys: Seq[DateTimeSpan]): (Double) => DateTimeSpan = {
    (d: Double) => {
      val (mins, maxs) = ys.map(y => (y.min, y.max)).unzip
      val minInterpolated = DateTimeStamp.createInterpolator(xs, mins)
      val maxInterpolated = DateTimeStamp.createInterpolator(xs, maxs)
      DateTimeSpan((minInterpolated(d).asInstanceOf[DateTimeStamp], maxInterpolated(d).asInstanceOf[DateTimeStamp]))
    }
  }

  override protected def typedReduce(ys: Seq[DateTimeSpan]): DateTimeSpan = {
    val minnest = DateTimeStamp(ys.map(_.min.asDouble).min)
    val maxxest = DateTimeStamp(ys.map(_.max.asDouble).max)
    DateTimeSpan((minnest, maxxest))
  }

}

