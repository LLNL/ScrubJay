package org.apache.spark.sql.scrubjaytypes

import java.time.format.DateTimeFormatter

import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.sql.scrubjaytypes.ScrubJayUDFParser._

class LocalDateTimeRangeStringUDT extends UserDefinedType[LocalDateTimeRangeType] {

  override def sqlType: DataType = StringType

  override def serialize(p: LocalDateTimeRangeType): UTF8String = LocalDateTimeRangeStringUDT.serialize(p)

  override def deserialize(datum: Any): LocalDateTimeRangeType = LocalDateTimeRangeStringUDT.deserialize(datum)

  override def userClass: Class[LocalDateTimeRangeType] = classOf[LocalDateTimeRangeType]

  private[spark] override def asNullable: LocalDateTimeRangeStringUDT = this
}

object LocalDateTimeRangeStringUDT {

  private val defaultPattern = "yyyy-MM-dd HH:mm:ss"
  private val dateFormatKey: String = "dateformat"
  private val defaultFormatter: DateTimeFormatter = DateTimeFormatter.ofPattern(defaultPattern)

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
        val values = s.substring(1, s.length - 1).split(",")
        new LocalDateTimeRangeType(
          LocalDateTimeStringUDT.deserialize(values(0), dateTimeFormatter),
          LocalDateTimeStringUDT.deserialize(values(1), dateTimeFormatter))
      }
      case s: String => {
        val values = s.substring(1, s.length - 1).split(",")
        new LocalDateTimeRangeType(
          LocalDateTimeStringUDT.deserialize(values(0), dateTimeFormatter),
          LocalDateTimeStringUDT.deserialize(values(1), dateTimeFormatter))
      }
    }
  }

  def parseStringUDF(df: DataFrame, structField: StructField, scrubjayParserMetadata: Metadata): Column = {
    val dateFormat = scrubjayParserMetadata.getStringOption(dateFormatKey).getOrElse(defaultPattern)
    val parseUDF = udf((s: String) => LocalDateTimeRangeStringUDT.deserialize(s, DateTimeFormatter.ofPattern(dateFormat)))
    parseUDF(df(structField.name))
  }
}
