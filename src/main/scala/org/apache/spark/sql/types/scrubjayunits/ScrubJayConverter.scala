package org.apache.spark.sql.types.scrubjayunits

import java.time.{LocalDateTime, ZoneOffset}

import org.apache.spark.sql.types
import org.apache.spark.sql.types._

abstract class ScrubJayConverter[A, B] extends Serializable {
  def a2b(a: A): B
  def b2a(b: B): A
}

class ScrubJayNumberDoubleConverter(toDouble: Double => Any) extends ScrubJayConverter[Any, Double] {
  def a2b(a: Any): Double = a match {
    case n: java.lang.Number => n.doubleValue
  }
  def b2a(b: Double): Any = toDouble(b)
}

class SJLocalDateTimeDoubleConverter extends ScrubJayConverter[Any, Double] {
  override def a2b(a: Any): Double = a.asInstanceOf[ScrubJayLocalDateTime_String].realValue
  override def b2a(b: Double): Any = LocalDateTime.ofEpochSecond(b.toInt, ((b % 1)*1e9).toInt, ZoneOffset.UTC)
}

object ScrubJayConverter {
  val SJLocalDateTimeDataType = new types.scrubjayunits.SJLocalDateTimeStringUDT

  def get(dataType: DataType): ScrubJayConverter[Any, Double] = dataType match {
    case numericType: NumericType => get(numericType)
    case SJLocalDateTimeDataType => new SJLocalDateTimeDoubleConverter
    case nonNumericType: DataType => throw new RuntimeException("Numeric conversion not supported for non-numeric type " + nonNumericType)
  }

  def get(numericType: NumericType): ScrubJayConverter[Any, Double] = numericType match {

    case IntegerType => new ScrubJayNumberDoubleConverter(d => d.round.toInt)
    case FloatType => new ScrubJayNumberDoubleConverter(d => d.toFloat)
    case DoubleType => new ScrubJayNumberDoubleConverter(d => d)

    case ByteType | DecimalType.ByteDecimal=>
      new ScrubJayNumberDoubleConverter(d => d.round.toByte)

    case ShortType | DecimalType.ShortDecimal=>
      new ScrubJayNumberDoubleConverter(d => d.round.toShort)

    case IntegerType | DecimalType.IntDecimal=>
      new ScrubJayNumberDoubleConverter(d => d.round.toInt)

    case LongType | DecimalType.LongDecimal=>
      new ScrubJayNumberDoubleConverter(d => d.round)

    case DecimalType.BigIntDecimal=>
      new ScrubJayNumberDoubleConverter(d => BigDecimal(d))

    case FloatType | DecimalType.FloatDecimal=>
      new ScrubJayNumberDoubleConverter(d => d.toFloat)

    case DoubleType | DecimalType.DoubleDecimal=>
      new ScrubJayNumberDoubleConverter(d => d)

  }
}
