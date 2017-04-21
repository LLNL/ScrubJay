package org.apache.spark.sql

import scrubjay.units.LocalDateTimeType

import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter


class LocalDateTimeStringUDT extends UserDefinedType[LocalDateTimeType] {

  override def sqlType: DataType = StringType

  override def serialize(p: LocalDateTimeType): UTF8String = LocalDateTimeStringUDT.serialize(p)

  override def deserialize(datum: Any): LocalDateTimeType = LocalDateTimeStringUDT.deserialize(datum)

  override def userClass: Class[LocalDateTimeType] = classOf[LocalDateTimeType]

  private[spark] override def asNullable: LocalDateTimeStringUDT = this
}

object LocalDateTimeStringUDT {

  private val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")

  def serialize(p: LocalDateTimeType): UTF8String = {
    UTF8String.fromString(p.value.format(formatter))
  }

  def deserialize(datum: Any): LocalDateTimeType = {
    datum match {
      case utf8: UTF8String => {
        val s = utf8.toString
        new LocalDateTimeType(LocalDateTime.parse(s, formatter))
      }
      case s: String => {
        new LocalDateTimeType(LocalDateTime.parse(s, formatter))
      }
    }
  }
}