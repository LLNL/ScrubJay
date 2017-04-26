package org.apache.spark.sql.types.scrubjayunits

import java.time.format.DateTimeFormatter

import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.unsafe.types.UTF8String

@SQLUserDefinedType(udt = classOf[ScrubJayLocalDateTimeRange_String.LocalDateTimeRangeStringUDT])
class ScrubJayLocalDateTimeRange_String(val start: ScrubJayLocalDateTime_String, val end: ScrubJayLocalDateTime_String)
  extends ScrubJayUnits {

  override def toString: String = {
    "LocalDateTimeRange(" + start + "," + end + ")"
  }

  override def equals(other: Any): Boolean = other match {
    case that: ScrubJayLocalDateTimeRange_String => this.start == that.start && this.end == that.end
    case _ => false
  }

  def discretize(milliseconds: Double): Array[ScrubJayLocalDateTime_String] = {
    val timeIterator: Iterator[ScrubJayLocalDateTime_String] = Iterator.iterate(start) { current =>
      new ScrubJayLocalDateTime_String(current.value.plusNanos((milliseconds * 1000000).toLong))
    }
    timeIterator.takeWhile(_ < end).toArray
  }
}

object ScrubJayLocalDateTimeRange_String {

  private val defaultPattern = "yyyy-MM-dd HH:mm:ss"
  private val dateFormatKey: String = "dateformat"
  private val defaultFormatter: DateTimeFormatter = DateTimeFormatter.ofPattern(defaultPattern)

  class LocalDateTimeRangeStringUDT extends UserDefinedType[ScrubJayLocalDateTimeRange_String] {
    override def sqlType: DataType = StringType
    override def serialize(p: ScrubJayLocalDateTimeRange_String): UTF8String = ScrubJayLocalDateTimeRange_String.serialize(p)
    override def deserialize(datum: Any): ScrubJayLocalDateTimeRange_String = ScrubJayLocalDateTimeRange_String.deserialize(datum)
    override def userClass: Class[ScrubJayLocalDateTimeRange_String] = classOf[ScrubJayLocalDateTimeRange_String]
  }

  def serialize(p: ScrubJayLocalDateTimeRange_String, dateTimeFormatter: DateTimeFormatter = defaultFormatter): UTF8String = {
    UTF8String.fromString("[" +
      Array(
        ScrubJayLocalDateTime_String.serialize(p.start),
        ScrubJayLocalDateTime_String.serialize(p.end)).mkString(",")
      + "]"
    )
  }

  def deserialize(datum: Any, dateTimeFormatter: DateTimeFormatter = defaultFormatter): ScrubJayLocalDateTimeRange_String = {
    datum match {
      case utf8: UTF8String => {
        val s = utf8.toString
        val values = s.substring(1, s.length - 1).split(",")
        new ScrubJayLocalDateTimeRange_String(
          ScrubJayLocalDateTime_String.deserialize(values(0), dateTimeFormatter),
          ScrubJayLocalDateTime_String.deserialize(values(1), dateTimeFormatter))
      }
      case s: String => {
        val values = s.substring(1, s.length - 1).split(",")
        new ScrubJayLocalDateTimeRange_String(
          ScrubJayLocalDateTime_String.deserialize(values(0), dateTimeFormatter),
          ScrubJayLocalDateTime_String.deserialize(values(1), dateTimeFormatter))
      }
    }
  }

  def parseStringUDF(df: DataFrame, structField: StructField, scrubjayParserMetadata: Metadata): Column = {
    val dateFormat = scrubjayParserMetadata.getElementOrElse(dateFormatKey, defaultPattern)
    val parseUDF = udf((s: String) => deserialize(s, DateTimeFormatter.ofPattern(dateFormat)))
    parseUDF(df(structField.name))
  }
}
