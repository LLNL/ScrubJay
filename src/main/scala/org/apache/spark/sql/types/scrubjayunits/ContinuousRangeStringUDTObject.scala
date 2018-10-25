// Copyright 2018 Lawrence Livermore National Security, LLC and other
// ScrubJay Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: (Apache-2.0 OR MIT)

package org.apache.spark.sql.types.scrubjayunits

import org.apache.spark.sql.catalyst.InternalRow
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

trait AverageAggregator[T] extends Aggregator[T]

object AverageAggregatorInt extends AverageAggregator[Int] {
  override def aggregate(xs: Seq[Int]): Int = (xs.sum.toDouble / xs.length).toInt
}

object AverageAggregatorDouble extends AverageAggregator[Double] {
  override def aggregate(xs: Seq[Double]): Double = xs.sum / xs.length
}

