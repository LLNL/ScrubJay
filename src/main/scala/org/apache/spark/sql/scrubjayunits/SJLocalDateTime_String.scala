package org.apache.spark.sql.scrubjayunits

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import org.apache.spark.sql.types.SQLUserDefinedType
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

import org.apache.spark.sql.scrubjayunits.ScrubJayUDFParser._


@SQLUserDefinedType(udt = classOf[SJLocalDateTime_String.SJLocalDateTimeUDT])
class SJLocalDateTime_String(val value: LocalDateTime)
  extends ScrubJayUnits {

  override def toString: String = {
    "LocalDateTime(" + value + ")"
  }

  override def equals(other: Any): Boolean = other match {
    case that: SJLocalDateTime_String => this.value == that.value
    case _ => false
  }

  def <(that: SJLocalDateTime_String): Boolean = {
    value.isBefore(that.value)
  }
}

object SJLocalDateTime_String {

  private val defaultPattern = "yyyy-MM-dd HH:mm:ss"
  private val dateFormatKey: String = "dateformat"
  private val defaultFormatter: DateTimeFormatter = DateTimeFormatter.ofPattern(defaultPattern)

  class SJLocalDateTimeUDT extends UserDefinedType[SJLocalDateTime_String] {
    override def sqlType: DataType = StringType
    override def serialize(p: SJLocalDateTime_String): UTF8String = SJLocalDateTime_String.serialize(p)
    override def deserialize(datum: Any): SJLocalDateTime_String = SJLocalDateTime_String.deserialize(datum)
    override def userClass: Class[SJLocalDateTime_String] = classOf[SJLocalDateTime_String]
  }

  def serialize(p: SJLocalDateTime_String, dateTimeFormatter: DateTimeFormatter = defaultFormatter): UTF8String = {
    UTF8String.fromString(p.value.format(dateTimeFormatter))
  }

  def deserialize(datum: Any, dateTimeFormatter: DateTimeFormatter = defaultFormatter): SJLocalDateTime_String = {
    datum match {
      case utf8: UTF8String => {
        val s = utf8.toString
        new SJLocalDateTime_String(LocalDateTime.parse(s, dateTimeFormatter))
      }
      case s: String => {
        new SJLocalDateTime_String(LocalDateTime.parse(s, dateTimeFormatter))
      }
    }
  }

  def parseStringUDF(metadata: Metadata): UserDefinedFunction = {
    val dateFormat = metadata.getStringOption(dateFormatKey).getOrElse(defaultPattern)
    udf((s: String) => SJLocalDateTimeRange_String.deserialize(s, DateTimeFormatter.ofPattern(dateFormat)))
  }
}
