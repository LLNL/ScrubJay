// Copyright 2018 Lawrence Livermore National Security, LLC and other
// ScrubJay Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: (Apache-2.0 OR MIT)

package org.apache.spark.sql.types.scrubjayunits

import org.apache.spark.sql.types
import org.apache.spark.sql.types._

trait ScrubJayArithmetic extends Serializable {
  def +(left: Any, Right: Any): Any
  def -(left: Any, Right: Any): Any
  def /(left: Any, Right: Any): Any
  def *(left: Any, Right: Any): Any
}

class ScrubJayArithmeticNumeric(dataType: DataType)
  extends ScrubJayArithmetic {

  val converter: ScrubJayConverter[Any, Double] = ScrubJayConverter.get(dataType)

  override def +(left:Any, right: Any) = converter.b2a(converter.a2b(left) + converter.a2b(right))
  override def -(left:Any, right: Any) = converter.b2a(converter.a2b(left) - converter.a2b(right))
  override def /(left:Any, right: Any) = converter.b2a(converter.a2b(left) / converter.a2b(right))
  override def *(left:Any, right: Any) = converter.b2a(converter.a2b(left) * converter.a2b(right))
}

object ScrubJayArithmetic {
  val SJLocalDateTimeDataType = new types.scrubjayunits.SJLocalDateTimeStringUDT
  def get(dataType: DataType): ScrubJayArithmetic = dataType match {
    case numericType: NumericType => new ScrubJayArithmeticNumeric(numericType)
    case SJLocalDateTimeDataType => new ScrubJayArithmeticNumeric(SJLocalDateTimeDataType)
    case nonNumericType => throw new RuntimeException("Arithmetic not supported for non-numeric type " + nonNumericType)
  }
}