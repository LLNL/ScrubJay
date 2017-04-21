package org.apache.spark.sql.scrubjaytypes

import java.time.LocalDateTime

import org.apache.spark.sql.types.SQLUserDefinedType

@SQLUserDefinedType(udt = classOf[LocalDateTimeStringUDT])
class LocalDateTimeType(val value: LocalDateTime) extends Serializable {

  override def toString: String = {
    "LocalDateTime(" + value + ")"
  }

  override def equals(other: Any): Boolean = other match {
    case that: LocalDateTimeType => this.value == that.value
    case _ => false
  }

  def <(that: LocalDateTimeType): Boolean = {
    value.isBefore(that.value)
  }
}
