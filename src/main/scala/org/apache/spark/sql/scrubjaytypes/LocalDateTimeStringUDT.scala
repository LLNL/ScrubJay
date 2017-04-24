package org.apache.spark.sql.scrubjaytypes

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

import org.apache.spark.sql.scrubjaytypes.ScrubJayUDFParser._

class LocalDateTimeStringUDT extends UserDefinedType[LocalDateTimeType] {

  override def sqlType: DataType = StringType

  override def serialize(p: LocalDateTimeType): UTF8String = LocalDateTimeStringUDT.serialize(p)

  override def deserialize(datum: Any): LocalDateTimeType = LocalDateTimeStringUDT.deserialize(datum)

  override def userClass: Class[LocalDateTimeType] = classOf[LocalDateTimeType]

  private[spark] override def asNullable: LocalDateTimeStringUDT = this
}

object LocalDateTimeStringUDT {

  private val defaultPattern = "yyyy-MM-dd HH:mm:ss"
  private val dateFormatKey: String = "dateformat"
  private val defaultFormatter: DateTimeFormatter = DateTimeFormatter.ofPattern(defaultPattern)

  def serialize(p: LocalDateTimeType, dateTimeFormatter: DateTimeFormatter = defaultFormatter): UTF8String = {
    UTF8String.fromString(p.value.format(dateTimeFormatter))
  }

  def deserialize(datum: Any, dateTimeFormatter: DateTimeFormatter = defaultFormatter): LocalDateTimeType = {
    datum match {
      case utf8: UTF8String => {
        val s = utf8.toString
        new LocalDateTimeType(LocalDateTime.parse(s, dateTimeFormatter))
      }
      case s: String => {
        new LocalDateTimeType(LocalDateTime.parse(s, dateTimeFormatter))
      }
    }
  }

  def parseStringUDF(metadata: Metadata): UserDefinedFunction = {
    val dateFormat = metadata.getStringOption(dateFormatKey).getOrElse(defaultPattern)
    udf((s: String) => LocalDateTimeRangeStringUDT.deserialize(s, DateTimeFormatter.ofPattern(dateFormat)))
  }
}