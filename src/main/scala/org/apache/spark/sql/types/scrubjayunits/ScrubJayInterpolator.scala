// Copyright 2018 Lawrence Livermore National Security, LLC and other
// ScrubJay Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: (Apache-2.0 OR MIT)

package org.apache.spark.sql.types.scrubjayunits

import org.apache.spark.sql.types._
import scrubjay.schema.ScrubJayUnitsSchema

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

class ScrubJayLinearInterpolatorNumeric(numericType: NumericType)
  extends ScrubJayLinearInterpolator {

  val converter: ScrubJayConverter[Any, Double] = ScrubJayConverter.get(numericType)

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
  def get(units: ScrubJayUnitsSchema, dataType: DataType): ScrubJayInterpolator = units.interpolator match {
    case "linear" => dataType match {
      case numericType: NumericType => new ScrubJayLinearInterpolatorNumeric(numericType)
      case nonNumericType => throw new RuntimeException("Linear interpolation not supported for non-numeric type " + nonNumericType)
    }
    case "nearest" => new ScrubJayNearestInterpolatorAny

    // Default to nearest
    case _ => new ScrubJayNearestInterpolatorAny
  }

}
