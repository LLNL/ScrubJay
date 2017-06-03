package org.apache.spark.sql.types.scrubjayunits

import java.time.LocalDateTime

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

trait Aggregator[T] {
  def aggregate(xs: Seq[T]): T
}

// FIXME: should use type class instead of Any everywhere...

trait Interpolator {
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
  override def interpolate(points: Seq[(Double, Any)], x: Double): Any = new ScrubJayLocalDateTime_String(LocalDateTime.now())
}

object LinearInterpolatorString extends LinearInterpolator {
  override def interpolate(points: Seq[(Double, Any)], x: Double): Any = "fifty five"
}

object LinearInterpolatorInt extends LinearInterpolator {
  override def interpolate(points: Seq[(Double, Any)], x: Double): Any = 55
}

object LinearInterpolatorDouble extends LinearInterpolator {
  override def interpolate(points: Seq[(Double, Any)], x: Double): Any = 888.8
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
