package scrubjay.units

import scala.reflect._

abstract class Units[T <: Units[T]] extends Serializable {
  def getClassTag = classTag[T]
}

//case class NoUnits(v: Any) extends Units[NoUnits]

case class Identifier(v: Any) extends Units[Identifier]
case class Seconds(v: Double) extends Units[Seconds]
case class Timestamp(v: Double) extends Units[Timestamp]

case class Job(i: Int) extends Units[Job]
case class Node(i: Int) extends Units[Node]
