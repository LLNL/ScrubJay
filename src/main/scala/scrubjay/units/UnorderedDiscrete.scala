package scrubjay.units

import scrubjay.metabase.MetaDescriptor._
import scrubjay.units.UnitsTag.DomainType
import scrubjay.units.UnitsTag.DomainType.DomainType

case class UnorderedDiscrete(value: String) extends Units[String]

object UnorderedDiscrete extends UnitsTag[UnorderedDiscrete, String] {

  override val domainType: DomainType = DomainType.POINT

  override def convert(value: Any, metaUnits: MetaUnits): UnorderedDiscrete = UnorderedDiscrete(value.toString)

  override protected def createTypedInterpolator(xs: Seq[Double], ys: Seq[UnorderedDiscrete]): (Double) => UnorderedDiscrete = {
    (d: Double) => xs.zip(ys).minBy{case (x, _) => Math.abs(x - d)}._2
  }

  override protected def typedReduce(ys: Seq[UnorderedDiscrete]): UnorderedDiscrete = {
    ys.groupBy(i => i).maxBy(_._2.length)._1 // mode
  }
}
