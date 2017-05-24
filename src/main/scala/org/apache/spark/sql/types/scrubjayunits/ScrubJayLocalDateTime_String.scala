package org.apache.spark.sql.types.scrubjayunits

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String


@SQLUserDefinedType(udt = classOf[ScrubJayLocalDateTime_String.SJLocalDateTimeStringUDT])
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

object ScrubJayLocalDateTime_String extends ScrubJayUDTObject {

  private val defaultPattern = "yyyy-MM-dd HH:mm:ss"
  private val dateFormatKey: String = "dateformat"
  private val defaultFormatter: DateTimeFormatter = DateTimeFormatter.ofPattern(defaultPattern)

  class SJLocalDateTimeStringUDT
    extends UserDefinedType[ScrubJayLocalDateTime_String] {

    override def sqlType: DataType = ScrubJayLocalDateTime_String.sqlType
    override def serialize(p: ScrubJayLocalDateTime_String): UTF8String = ScrubJayLocalDateTime_String.serialize(p)
    override def deserialize(datum: Any): ScrubJayLocalDateTime_String = ScrubJayLocalDateTime_String.deserialize(datum)
    override def userClass: Class[ScrubJayLocalDateTime_String] = classOf[ScrubJayLocalDateTime_String]

  }

  override def sqlType: DataType = StringType

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

  def parseStringUDF(df: DataFrame, structField: StructField, scrubjayParserMetadata: Metadata): Column = {
    val dateFormat = scrubjayParserMetadata.getElementOrElse(dateFormatKey, defaultPattern)
    val parseUDF = udf((s: String) => deserialize(s, DateTimeFormatter.ofPattern(dateFormat)))
    parseUDF(df(structField.name))
  }
}
