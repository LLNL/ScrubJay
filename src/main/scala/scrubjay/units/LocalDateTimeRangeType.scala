package scrubjay.units

import org.apache.spark.sql.LocalDateTimeRangeStringUDT
import org.apache.spark.sql.types.SQLUserDefinedType

@SQLUserDefinedType(udt = classOf[LocalDateTimeRangeStringUDT])
class LocalDateTimeRangeType(val start: LocalDateTimeType, val end: LocalDateTimeType)
  extends Serializable {

  override def toString: String = {
    "LocalDateTimeRange(" + start + "," + end + ")"
  }

  override def equals(other: Any): Boolean = other match {
    case that: LocalDateTimeRangeType => this.start == that.start && this.end == that.end
    case _ => false
  }

  def discretize(milliseconds: Double): Array[LocalDateTimeType] = {
    val timeIterator: Iterator[LocalDateTimeType] = Iterator.iterate(start){ current =>
      new LocalDateTimeType(current.value.plusNanos((milliseconds*1000000).toLong))
    }
    timeIterator.takeWhile(_ < end).toArray
  }

}
