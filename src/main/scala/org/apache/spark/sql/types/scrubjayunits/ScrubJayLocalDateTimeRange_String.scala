package org.apache.spark.sql.types.scrubjayunits

import java.time.format.DateTimeFormatter

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Column, DataFrame, types}
import org.apache.spark.unsafe.types.UTF8String

@SQLUserDefinedType(udt = classOf[ScrubJayLocalDateTimeRange_String.SJLocalDateTimeRangeStringUDT])
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

object ScrubJayLocalDateTimeRange_String extends ScrubJayUDTObject {

  private val defaultPattern = "yyyy-MM-dd HH:mm:ss"
  private val dateFormatKey: String = "dateformat"
  private val defaultFormatter: DateTimeFormatter = DateTimeFormatter.ofPattern(defaultPattern)

  class SJLocalDateTimeRangeStringUDT
    extends UserDefinedType[ScrubJayLocalDateTimeRange_String]
      with ContinuousRangeStringUDTObject {

    override def sqlType: DataType = ScrubJayLocalDateTimeRange_String.sqlType
    override def serialize(p: ScrubJayLocalDateTimeRange_String): UTF8String = ScrubJayLocalDateTimeRange_String.serialize(p)
    override def deserialize(datum: Any): ScrubJayLocalDateTimeRange_String = ScrubJayLocalDateTimeRange_String.deserialize(datum)
    override def userClass: Class[ScrubJayLocalDateTimeRange_String] = classOf[ScrubJayLocalDateTimeRange_String]

    override def explodedType: DataType = new types.scrubjayunits.ScrubJayLocalDateTime_String.SJLocalDateTimeStringUDT

    override def explodedValues(s: UTF8String, interval: Double): Array[InternalRow] = {
      deserialize(s).discretize(interval).map(instant => InternalRow(ScrubJayLocalDateTime_String.serialize(instant)))
    }
  }

  override def sqlType: DataType = StringType

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
