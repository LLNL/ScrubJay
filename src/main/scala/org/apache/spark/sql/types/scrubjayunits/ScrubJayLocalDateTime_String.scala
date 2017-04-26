package org.apache.spark.sql.types.scrubjayunits

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String


@SQLUserDefinedType(udt = classOf[ScrubJayLocalDateTime_String.SJLocalDateTimeUDT])
class ScrubJayLocalDateTime_String(val value: LocalDateTime)
  extends ScrubJayUnits {

  override def toString: String = {
    "LocalDateTime(" + value + ")"
  }

  override def equals(other: Any): Boolean = other match {
    case that: ScrubJayLocalDateTime_String => this.value == that.value
    case _ => false
  }

  def <(that: ScrubJayLocalDateTime_String): Boolean = {
    value.isBefore(that.value)
  }
}

object ScrubJayLocalDateTime_String {

  private val defaultPattern = "yyyy-MM-dd HH:mm:ss"
  private val dateFormatKey: String = "dateformat"
  private val defaultFormatter: DateTimeFormatter = DateTimeFormatter.ofPattern(defaultPattern)

  class SJLocalDateTimeUDT extends UserDefinedType[ScrubJayLocalDateTime_String] {
    override def sqlType: DataType = StringType
    override def serialize(p: ScrubJayLocalDateTime_String): UTF8String = ScrubJayLocalDateTime_String.serialize(p)
    override def deserialize(datum: Any): ScrubJayLocalDateTime_String = ScrubJayLocalDateTime_String.deserialize(datum)
    override def userClass: Class[ScrubJayLocalDateTime_String] = classOf[ScrubJayLocalDateTime_String]
  }

  def serialize(p: ScrubJayLocalDateTime_String, dateTimeFormatter: DateTimeFormatter = defaultFormatter): UTF8String = {
    UTF8String.fromString(p.value.format(dateTimeFormatter))
  }

  def deserialize(datum: Any, dateTimeFormatter: DateTimeFormatter = defaultFormatter): ScrubJayLocalDateTime_String = {
    datum match {
      case utf8: UTF8String => {
        val s = utf8.toString
        new ScrubJayLocalDateTime_String(LocalDateTime.parse(s, dateTimeFormatter))
      }
      case s: String => {
        new ScrubJayLocalDateTime_String(LocalDateTime.parse(s, dateTimeFormatter))
      }
    }
  }

  def parseStringUDF(metadata: Metadata): UserDefinedFunction = {
    val dateFormat = metadata.getElementOrElse(dateFormatKey, defaultPattern)
    udf((s: String) => ScrubJayLocalDateTimeRange_String.deserialize(s, DateTimeFormatter.ofPattern(dateFormat)))
  }
}
