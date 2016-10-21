package scrubjay.units

import scrubjay.metabase.MetaDescriptor._
import scrubjay.units.UnitsTag.DomainType
import scrubjay.units.UnitsTag.DomainType.DomainType

import scala.reflect._

case class Identifier(value: String) extends Units[String]

object Identifier extends UnitsTag[Identifier] {

  override val rawValueClassTag = classTag[String]
  override val domainType: DomainType = DomainType.UNKNOWN

  override def convert(value: Any, metaUnits: MetaUnits): Identifier = Identifier(value.toString)
  override def createInterpolator(xs: Seq[Double], ys: Seq[Identifier]): (Double) => Identifier = {
    (d: Double) => xs.zip(ys).minBy{case (x, y) => Math.abs(x - d)}._2
  }
}
