package org.apache.spark.sql.scrubjayunits

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import org.apache.spark.sql.types.SQLUserDefinedType
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

import org.apache.spark.sql.scrubjayunits.ScrubJayUDFParser._


@SQLUserDefinedType(udt = classOf[SJLocalDateTime.SJLocalDateTimeUDT])
class SJLocalDateTime(val value: LocalDateTime)
  extends ScrubJayUnits {

  override def toString: String = {
    "LocalDateTime(" + value + ")"
  }

  override def equals(other: Any): Boolean = other match {
    case that: SJLocalDateTime => this.value == that.value
    case _ => false
  }

  def <(that: SJLocalDateTime): Boolean = {
    value.isBefore(that.value)
  }
}

object SJLocalDateTime {

  private val defaultPattern = "yyyy-MM-dd HH:mm:ss"
  private val dateFormatKey: String = "dateformat"
  private val defaultFormatter: DateTimeFormatter = DateTimeFormatter.ofPattern(defaultPattern)

  class SJLocalDateTimeUDT extends UserDefinedType[SJLocalDateTime] {
    override def sqlType: DataType = StringType
    override def serialize(p: SJLocalDateTime): UTF8String = SJLocalDateTime.serialize(p)
    override def deserialize(datum: Any): SJLocalDateTime = SJLocalDateTime.deserialize(datum)
    override def userClass: Class[SJLocalDateTime] = classOf[SJLocalDateTime]
  }

  def serialize(p: SJLocalDateTime, dateTimeFormatter: DateTimeFormatter = defaultFormatter): UTF8String = {
    UTF8String.fromString(p.value.format(dateTimeFormatter))
  }

  def deserialize(datum: Any, dateTimeFormatter: DateTimeFormatter = defaultFormatter): SJLocalDateTime = {
    datum match {
      case utf8: UTF8String => {
        val s = utf8.toString
        new SJLocalDateTime(LocalDateTime.parse(s, dateTimeFormatter))
      }
      case s: String => {
        new SJLocalDateTime(LocalDateTime.parse(s, dateTimeFormatter))
      }
    }
  }

  def parseStringUDF(metadata: Metadata): UserDefinedFunction = {
    val dateFormat = metadata.getStringOption(dateFormatKey).getOrElse(defaultPattern)
    udf((s: String) => LocalDateTimeRange.deserialize(s, DateTimeFormatter.ofPattern(dateFormat)))
  }
}
