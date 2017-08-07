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

object ScrubJayNearestInterpolatorAny extends ScrubJayNearestInterpolator {
  override def interpolate(points: Seq[(Double, Any)], x: Double): Any = {
    points.minBy{case (px, py) => Math.abs(px - x)}._2
  }
}

object ScrubJayLinearInterpolatorDouble extends ScrubJayLinearInterpolator {

  def doubleInterpolate(points: Seq[(Double, Double)], x: Double): Double = {
    if (points.length == 1) {
      points.head._2
    } else {
      val (xs, ys) = points.unzip
      val xdv = breeze.linalg.Vector(xs: _*)
      val ydv = breeze.linalg.Vector(ys: _*)
      val interpolator = breeze.interpolation.LinearInterpolator(xdv, ydv)
      interpolator(x)
    }
  }

  override def interpolate(points: Seq[(Double, Any)], x: Double): Any = {
    doubleInterpolate(points.map{case (px: Double, py: Double) => (px, py)}, x)
  }
}

object ScrubJayLinearInterpolatorInt extends ScrubJayLinearInterpolator {
  override def interpolate(points: Seq[(Double, Any)], x: Double): Any = {
    val dpoints = points.map{case (x, y: Int) => (x, y.toDouble)}
    val dval = ScrubJayLinearInterpolatorDouble.doubleInterpolate(dpoints, x)
    dval.round.toInt
  }
}

object ScrubJayLinearInterpolatorSJLocalDateTime extends ScrubJayLinearInterpolator {
  override def interpolate(points: Seq[(Double, Any)], x: Double): Any = {
    val dpoints = points.map{case (x, y) => (x, y.asInstanceOf[ScrubJayLocalDateTime_String].realValue)}
    val dval = ScrubJayLinearInterpolatorDouble.doubleInterpolate(dpoints, x)
    new ScrubJayLocalDateTime_String(LocalDateTime.ofEpochSecond(dval.toInt, ((dval % 1)*1e9).toInt, ZoneOffset.UTC))
  }
}

object Interpolator {
  val SJLocalDateTimeDataType = new types.scrubjayunits.SJLocalDateTimeStringUDT
  def get(units: ScrubJayUnitsField, dataType: DataType): ScrubJayInterpolator = (units, dataType) match {

    // Nearest interpolator
    case (ScrubJayUnitsField(_, _, _, "nearest", _), _) => ScrubJayNearestInterpolatorAny

    // Linear interpolators for base types
    case (ScrubJayUnitsField(_, _, _, "linear", _), SJLocalDateTimeDataType) => ScrubJayLinearInterpolatorSJLocalDateTime
    case (ScrubJayUnitsField(_, _, _, "linear", _), IntegerType) => ScrubJayLinearInterpolatorInt
    case (ScrubJayUnitsField(_, _, _, "linear", _), DoubleType) => ScrubJayLinearInterpolatorDouble

    // Default interpolators for base types
    case (_, SJLocalDateTimeDataType) => ScrubJayLinearInterpolatorSJLocalDateTime
    case (_, StringType) => ScrubJayNearestInterpolatorAny
    case (_, IntegerType) => ScrubJayLinearInterpolatorInt
    case (_, DoubleType) => ScrubJayLinearInterpolatorDouble

  }
}
