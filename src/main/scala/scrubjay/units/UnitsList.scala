
package org.apache.spark.sql.udt

import org.apache.spark.sql.catalyst.ScalaReflectionLock
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.catalyst.util.{ArrayData, GenericArrayData}
import org.apache.spark.sql.types._

import scala.reflect.runtime.universe.typeTag


class WowStringType private() extends AtomicType {
  private[sql] type InternalType = Long

  @transient private[sql] lazy val tag = ScalaReflectionLock.synchronized { typeTag[InternalType] }

  private[sql] val ordering = implicitly[Ordering[InternalType]]

  override def defaultSize: Int = 8

  private[spark] override def asNullable: WowStringType = this
}

@SQLUserDefinedType(udt = classOf[WowStringUDT])
private[sql] class WowString(val s: String) extends Serializable {
  def wow: String = "WOW" + s + "WOW"
}

private [sql] class WowStringUDT extends UserDefinedType[WowString] {

  override def sqlType: DataType = ???

  override def serialize(obj: WowString): Any = ???

  override def deserialize(datum: Any): WowString = ???

  override def userClass: Class[WowString] = ???
}
