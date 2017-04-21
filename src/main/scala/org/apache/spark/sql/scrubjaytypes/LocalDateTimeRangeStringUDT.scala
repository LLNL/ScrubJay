package org.apache.spark.sql.scrubjaytypes

import java.time.format.DateTimeFormatter

import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String


class LocalDateTimeRangeStringUDT extends UserDefinedType[LocalDateTimeRangeType] {

  override def sqlType: DataType = StringType

  override def serialize(p: LocalDateTimeRangeType): UTF8String = LocalDateTimeRangeStringUDT.serialize(p)

  override def deserialize(datum: Any): LocalDateTimeRangeType = LocalDateTimeRangeStringUDT.deserialize(datum)

  override def userClass: Class[LocalDateTimeRangeType] = classOf[LocalDateTimeRangeType]

  private[spark] override def asNullable: LocalDateTimeRangeStringUDT = this
}

object LocalDateTimeRangeStringUDT {

  private val defaultFormatter: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")

  def serialize(p: LocalDateTimeRangeType, dateTimeFormatter: DateTimeFormatter = defaultFormatter): UTF8String = {
    UTF8String.fromString("[" +
      Array(
        LocalDateTimeStringUDT.serialize(p.start),
        LocalDateTimeStringUDT.serialize(p.end)).mkString(",")
      + "]"
    )
  }

  def deserialize(datum: Any, dateTimeFormatter: DateTimeFormatter = defaultFormatter): LocalDateTimeRangeType = {
    datum match {
      case utf8: UTF8String => {
        val s = utf8.toString
        val values = s.substring(1, s.length-1).split(",")
        new LocalDateTimeRangeType(
          LocalDateTimeStringUDT.deserialize(values(0), dateTimeFormatter),
          LocalDateTimeStringUDT.deserialize(values(1), dateTimeFormatter))
      }
      case s: String => {
        val values = s.substring(1, s.length-1).split(",")
        new LocalDateTimeRangeType(
          LocalDateTimeStringUDT.deserialize(values(0), dateTimeFormatter),
          LocalDateTimeStringUDT.deserialize(values(1), dateTimeFormatter))
      }
    }
  }
}
