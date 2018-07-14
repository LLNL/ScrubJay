package org.apache.spark.sql.types.scrubjayunits

import java.time.{LocalDateTime, ZoneOffset}

import org.apache.spark.sql.types
import org.apache.spark.sql.types._
import scrubjay.datasetid.ScrubJayUnitsField

// FIXME: should use type class instead of Any everywhere...
trait ScrubJayInterpolator extends Serializable {
  def interpolate(points: Seq[(Double, Any)], x: Double): Any
}

trait ScrubJayLinearInterpolator extends ScrubJayInterpolator
trait ScrubJayNearestInterpolator extends ScrubJayInterpolator

class ScrubJayNearestInterpolatorAny extends ScrubJayNearestInterpolator {
  override def interpolate(points: Seq[(Double, Any)], x: Double): Any = {
    points.minBy{case (px, py) => Math.abs(px - x)}._2
  }
}

abstract class Converter[A, B] extends Serializable {
  def a2b(a: A): B
  def b2a(b: B): A
}

class NumberDoubleConverter(toDouble: Double => Any) extends Converter[Any, Double] {
  def a2b(a: Any): Double = a match {
    case n: java.lang.Number => n.doubleValue
  }
  def b2a(b: Double): Any = toDouble(b)
}

class ScrubJayLinearInterpolatorNumeric(converter: Converter[Any, Double])
  extends ScrubJayLinearInterpolator {

  override def interpolate(points: Seq[(Double, Any)], x: Double): Any = {

    val dpoints: Seq[(Double, Double)] = points.map{
      case (x, y) => (x, converter.a2b(y))
    }

    if (dpoints.length == 1) {
      converter.b2a(dpoints.head._2)
    } else {
      val (xs, ys) = dpoints.unzip
      val xdv = breeze.linalg.Vector(xs: _*)
      val ydv = breeze.linalg.Vector(ys: _*)
      val interpolator = breeze.interpolation.LinearInterpolator(xdv, ydv)
      converter.b2a(interpolator(x))
    }
  }
}

object Interpolator {
  val SJLocalDateTimeDataType = new types.scrubjayunits.SJLocalDateTimeStringUDT
  def get(units: ScrubJayUnitsField, dataType: DataType): ScrubJayInterpolator = (units, dataType) match {

    // Nearest interpolator
    case (ScrubJayUnitsField(_, _, _, "nearest", _), _) => new ScrubJayNearestInterpolatorAny

    // Linear interpolators for base types
    case (ScrubJayUnitsField(_, _, _, "linear", _), IntegerType) => new ScrubJayLinearInterpolatorNumeric(new NumberDoubleConverter(d => d.round.toInt))
    case (ScrubJayUnitsField(_, _, _, "linear", _), FloatType) => new ScrubJayLinearInterpolatorNumeric(new NumberDoubleConverter(d => d.toFloat))
    case (ScrubJayUnitsField(_, _, _, "linear", _), DoubleType) => new ScrubJayLinearInterpolatorNumeric(new NumberDoubleConverter(d => d))

    // Default interpolators for base types
    case (_, StringType) => new ScrubJayNearestInterpolatorAny

    case (_, SJLocalDateTimeDataType) => new ScrubJayLinearInterpolatorNumeric(new Converter[Any, Double] {
      override def a2b(a: Any): Double = a.asInstanceOf[ScrubJayLocalDateTime_String].realValue
      override def b2a(b: Double): Any = LocalDateTime.ofEpochSecond(b.toInt, ((b % 1)*1e9).toInt, ZoneOffset.UTC)
    })

    case (_, ByteType) | (_, DecimalType.ByteDecimal) =>
      new ScrubJayLinearInterpolatorNumeric(new NumberDoubleConverter(d => d.round.toByte))

    case (_, ShortType) | (_, DecimalType.ShortDecimal) =>
      new ScrubJayLinearInterpolatorNumeric(new NumberDoubleConverter(d => d.round.toShort))

    case (_, IntegerType) | (_, DecimalType.IntDecimal) =>
      new ScrubJayLinearInterpolatorNumeric(new NumberDoubleConverter(d => d.round.toInt))

    case (_, LongType) | (_, DecimalType.LongDecimal) | (_, DecimalType.BigIntDecimal) =>
      new ScrubJayLinearInterpolatorNumeric(new NumberDoubleConverter(d => d.round))

    case (_, FloatType) | (_, DecimalType.FloatDecimal) =>
      new ScrubJayLinearInterpolatorNumeric(new NumberDoubleConverter(d => d.toFloat))

    case (_, DoubleType) | (_, DecimalType.DoubleDecimal) =>
      new ScrubJayLinearInterpolatorNumeric(new NumberDoubleConverter(d => d))
  }
}
