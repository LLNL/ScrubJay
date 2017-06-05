package org.apache.spark.sql.types.scrubjayunits

import java.time.{LocalDateTime, ZoneOffset}

import org.apache.spark.sql.types
import org.apache.spark.sql.types._
import scrubjay.datasetid.ScrubJayUnitsField

// FIXME: should use type class instead of Any everywhere...
trait ScrubJayInterpolator extends Serializable {
  def interpolate(points: Seq[(Double, Any)], x: Double): Any
}

trait LinearInterpolator extends ScrubJayInterpolator

object LinearInterpolatorString extends LinearInterpolator {
  override def interpolate(points: Seq[(Double, Any)], x: Double): Any = "fifty five"
}

object LinearInterpolatorDouble extends LinearInterpolator {

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

object LinearInterpolatorInt extends LinearInterpolator {
  override def interpolate(points: Seq[(Double, Any)], x: Double): Any = {
    val dpoints = points.map{case (x, y: Int) => (x, y.toDouble)}
    val dval = LinearInterpolatorDouble.doubleInterpolate(dpoints, x)
    dval.round.toInt
  }
}

object LinearInterpolatorSJLocalDateTime extends LinearInterpolator {
  override def interpolate(points: Seq[(Double, Any)], x: Double): Any = {
    val dpoints = points.map{case (x, y) => (x, y.asInstanceOf[ScrubJayLocalDateTime_String].realValue)}
    val dval = LinearInterpolatorDouble.doubleInterpolate(dpoints, x)
    new ScrubJayLocalDateTime_String(LocalDateTime.ofEpochSecond(dval.toInt, ((dval % 1)*1e9).toInt, ZoneOffset.UTC))
  }
}

object Interpolator {
  val SJLocalDateTimeDataType = new types.scrubjayunits.ScrubJayLocalDateTime_String.SJLocalDateTimeStringUDT
  def get(units: ScrubJayUnitsField, dataType: DataType): ScrubJayInterpolator = (units, dataType) match {
    case (_, SJLocalDateTimeDataType) => LinearInterpolatorSJLocalDateTime
    case (_, StringType) => LinearInterpolatorString
    case (_, IntegerType) => LinearInterpolatorInt
    case (_, DoubleType) => LinearInterpolatorDouble
  }
}
