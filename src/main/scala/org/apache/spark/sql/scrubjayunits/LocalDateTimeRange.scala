package org.apache.spark.sql.scrubjayunits

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.types.SQLUserDefinedType
import java.time.format.DateTimeFormatter

import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.sql.scrubjayunits.ScrubJayUDFParser._

@SQLUserDefinedType(udt = classOf[LocalDateTimeRange.LocalDateTimeRangeStringUDT])
class LocalDateTimeRange(val start: SJLocalDateTime, val end: SJLocalDateTime)
  extends ScrubJayUnits {

  override def toString: String = {
    "LocalDateTimeRange(" + start + "," + end + ")"
  }

  override def equals(other: Any): Boolean = other match {
    case that: LocalDateTimeRange => this.start == that.start && this.end == that.end
    case _ => false
  }

  def discretize(milliseconds: Double): Array[SJLocalDateTime] = {
    val timeIterator: Iterator[SJLocalDateTime] = Iterator.iterate(start) { current =>
      new SJLocalDateTime(current.value.plusNanos((milliseconds * 1000000).toLong))
    }
    timeIterator.takeWhile(_ < end).toArray
  }
}

object LocalDateTimeRange {

  private val defaultPattern = "yyyy-MM-dd HH:mm:ss"
  private val dateFormatKey: String = "dateformat"
  private val defaultFormatter: DateTimeFormatter = DateTimeFormatter.ofPattern(defaultPattern)

  class LocalDateTimeRangeStringUDT extends UserDefinedType[LocalDateTimeRange] {
    override def sqlType: DataType = StringType
    override def serialize(p: LocalDateTimeRange): UTF8String = LocalDateTimeRange.serialize(p)
    override def deserialize(datum: Any): LocalDateTimeRange = LocalDateTimeRange.deserialize(datum)
    override def userClass: Class[LocalDateTimeRange] = classOf[LocalDateTimeRange]
  }

  def serialize(p: LocalDateTimeRange, dateTimeFormatter: DateTimeFormatter = defaultFormatter): UTF8String = {
    UTF8String.fromString("[" +
      Array(
        SJLocalDateTime.serialize(p.start),
        SJLocalDateTime.serialize(p.end)).mkString(",")
      + "]"
    )
  }

  def deserialize(datum: Any, dateTimeFormatter: DateTimeFormatter = defaultFormatter): LocalDateTimeRange = {
    datum match {
      case utf8: UTF8String => {
        val s = utf8.toString
        val values = s.substring(1, s.length - 1).split(",")
        new LocalDateTimeRange(
          SJLocalDateTime.deserialize(values(0), dateTimeFormatter),
          SJLocalDateTime.deserialize(values(1), dateTimeFormatter))
      }
      case s: String => {
        val values = s.substring(1, s.length - 1).split(",")
        new LocalDateTimeRange(
          SJLocalDateTime.deserialize(values(0), dateTimeFormatter),
          SJLocalDateTime.deserialize(values(1), dateTimeFormatter))
      }
    }
  }

  def parseStringUDF(df: DataFrame, structField: StructField, scrubjayParserMetadata: Metadata): Column = {
    val dateFormat = scrubjayParserMetadata.getStringOption(dateFormatKey).getOrElse(defaultPattern)
    val parseUDF = udf((s: String) => deserialize(s, DateTimeFormatter.ofPattern(dateFormat)))
    parseUDF(df(structField.name))
  }
}
