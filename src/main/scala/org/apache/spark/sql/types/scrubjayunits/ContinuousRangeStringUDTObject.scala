package org.apache.spark.sql.types.scrubjayunits

import java.time.{LocalDateTime, ZoneOffset}

import breeze.linalg.DenseVector
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

trait ContinuousRangeStringUDTObject {
  def explodedType: DataType
  def explodedValues(s: UTF8String, interval: Double): Array[InternalRow]
}

trait RealValued {
  def realValue: Double
}

trait Aggregator[T] extends Serializable {
  def aggregate(xs: Seq[T]): T
}

// FIXME: should use type class instead of Any everywhere...

trait Interpolator extends Serializable {
  def interpolate(points: Seq[(Double, Any)], x: Double): Any
}

trait AverageAggregator[T] extends Aggregator[T]
trait LinearInterpolator extends Interpolator

object AverageAggregatorInt extends AverageAggregator[Int] {
  override def aggregate(xs: Seq[Int]): Int = (xs.sum.toDouble / xs.length).toInt
}

object AverageAggregatorDouble extends AverageAggregator[Double] {
  override def aggregate(xs: Seq[Double]): Double = xs.sum / xs.length
}

object LinearInterpolatorSJLocalDateTime extends LinearInterpolator {
  override def interpolate(points: Seq[(Double, Any)], x: Double): Any = {
    val dpoints = points.map{case (x, y) => (x, y.asInstanceOf[ScrubJayLocalDateTime_String].realValue)}
    val dval = LinearInterpolatorDouble.interpolate(dpoints, x).asInstanceOf[Double]
    new ScrubJayLocalDateTime_String(LocalDateTime.ofEpochSecond(dval.toInt, ((dval - dval.toInt)*1e9).toInt, ZoneOffset.UTC))
  }
}

object LinearInterpolatorString extends LinearInterpolator {
  override def interpolate(points: Seq[(Double, Any)], x: Double): Any = "fifty five"
}

object LinearInterpolatorInt extends LinearInterpolator {
  override def interpolate(points: Seq[(Double, Any)], x: Double): Any = 55
}

object LinearInterpolatorDouble extends LinearInterpolator {
  override def interpolate(points: Seq[(Double, Any)], x: Double): Any = {
    if (points.length == 1) {
      points.head._2
    } else {
      val (xs, ys) = points.unzip
      val xdv = DenseVector(xs: _*)
      val ydv = DenseVector(ys.map(_.asInstanceOf[Double]): _*)
      val interpolator = breeze.interpolation.LinearInterpolator(xdv, ydv)
      interpolator(x)
    }
  }
}

object Interpolator {
  val SJLocalDateTimeDataType = new types.scrubjayunits.ScrubJayLocalDateTime_String.SJLocalDateTimeStringUDT
  def get(units: String, kind: String, dataType: DataType): Interpolator = (units, kind, dataType) match {
    case (_, _, SJLocalDateTimeDataType) => LinearInterpolatorSJLocalDateTime
    case (_, _, StringType) => LinearInterpolatorString
    case (_, _, IntegerType) => LinearInterpolatorInt
    case (_, _, DoubleType) => LinearInterpolatorDouble
  }
}
