package org.apache.spark.sql.types.scrubjayunits

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.DataType
import org.apache.spark.unsafe.types.UTF8String

trait ContinuousRangeStringUDTObject {
  def explodedType: DataType
  def explodedValues(s: UTF8String, interval: Double): Array[InternalRow]
}

