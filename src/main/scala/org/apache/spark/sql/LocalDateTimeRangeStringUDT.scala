package org.apache.spark.sql

import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

import scrubjay.units.LocalDateTimeRangeType


class LocalDateTimeRangeStringUDT extends UserDefinedType[LocalDateTimeRangeType] {

  override def sqlType: DataType = StringType

  override def serialize(p: LocalDateTimeRangeType): UTF8String = {
    UTF8String.fromString("[" +
      Array(
        LocalDateTimeStringUDT.serialize(p.start),
        LocalDateTimeStringUDT.serialize(p.end)).mkString(",")
      + "]"
    )
  }

  override def deserialize(datum: Any): LocalDateTimeRangeType = {
    datum match {
      case utf8: UTF8String => {
        val s = utf8.toString
        val values = s.substring(1, s.length-1).split(",")
        new LocalDateTimeRangeType(
          LocalDateTimeStringUDT.deserialize(values(0)),
          LocalDateTimeStringUDT.deserialize(values(1)))
      }
      case s: String => {
        val values = s.substring(1, s.length-1).split(",")
        new LocalDateTimeRangeType(
          LocalDateTimeStringUDT.deserialize(values(0)),
          LocalDateTimeStringUDT.deserialize(values(1)))
      }
    }
  }

  override def userClass: Class[LocalDateTimeRangeType] = classOf[LocalDateTimeRangeType]

  private[spark] override def asNullable: LocalDateTimeRangeStringUDT = this
}
