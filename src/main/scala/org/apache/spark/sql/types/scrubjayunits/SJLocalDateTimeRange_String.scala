package org.apache.spark.sql.types.scrubjayunits

import java.time.format.DateTimeFormatter

import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.unsafe.types.UTF8String

@SQLUserDefinedType(udt = classOf[SJLocalDateTimeRange_String.LocalDateTimeRangeStringUDT])
class SJLocalDateTimeRange_String(val start: SJLocalDateTime_String, val end: SJLocalDateTime_String)
  extends SJUnits {

  override def toString: String = {
    "LocalDateTimeRange(" + start + "," + end + ")"
  }

  override def equals(other: Any): Boolean = other match {
    case that: SJLocalDateTimeRange_String => this.start == that.start && this.end == that.end
    case _ => false
  }

  def discretize(milliseconds: Double): Array[SJLocalDateTime_String] = {
    val timeIterator: Iterator[SJLocalDateTime_String] = Iterator.iterate(start) { current =>
      new SJLocalDateTime_String(current.value.plusNanos((milliseconds * 1000000).toLong))
    }
    timeIterator.takeWhile(_ < end).toArray
  }
}

object SJLocalDateTimeRange_String {

  private val defaultPattern = "yyyy-MM-dd HH:mm:ss"
  private val dateFormatKey: String = "dateformat"
  private val defaultFormatter: DateTimeFormatter = DateTimeFormatter.ofPattern(defaultPattern)

  class LocalDateTimeRangeStringUDT extends UserDefinedType[SJLocalDateTimeRange_String] {
    override def sqlType: DataType = StringType
    override def serialize(p: SJLocalDateTimeRange_String): UTF8String = SJLocalDateTimeRange_String.serialize(p)
    override def deserialize(datum: Any): SJLocalDateTimeRange_String = SJLocalDateTimeRange_String.deserialize(datum)
    override def userClass: Class[SJLocalDateTimeRange_String] = classOf[SJLocalDateTimeRange_String]
  }

  def serialize(p: SJLocalDateTimeRange_String, dateTimeFormatter: DateTimeFormatter = defaultFormatter): UTF8String = {
    UTF8String.fromString("[" +
      Array(
        SJLocalDateTime_String.serialize(p.start),
        SJLocalDateTime_String.serialize(p.end)).mkString(",")
      + "]"
    )
  }

  def deserialize(datum: Any, dateTimeFormatter: DateTimeFormatter = defaultFormatter): SJLocalDateTimeRange_String = {
    datum match {
      case utf8: UTF8String => {
        val s = utf8.toString
        val values = s.substring(1, s.length - 1).split(",")
        new SJLocalDateTimeRange_String(
          SJLocalDateTime_String.deserialize(values(0), dateTimeFormatter),
          SJLocalDateTime_String.deserialize(values(1), dateTimeFormatter))
      }
      case s: String => {
        val values = s.substring(1, s.length - 1).split(",")
        new SJLocalDateTimeRange_String(
          SJLocalDateTime_String.deserialize(values(0), dateTimeFormatter),
          SJLocalDateTime_String.deserialize(values(1), dateTimeFormatter))
      }
    }
  }

  def parseStringUDF(df: DataFrame, structField: StructField, scrubjayParserMetadata: Metadata): Column = {
    val dateFormat = scrubjayParserMetadata.getElementOrElse(dateFormatKey, defaultPattern)
    val parseUDF = udf((s: String) => deserialize(s, DateTimeFormatter.ofPattern(dateFormat)))
    parseUDF(df(structField.name))
  }
}
