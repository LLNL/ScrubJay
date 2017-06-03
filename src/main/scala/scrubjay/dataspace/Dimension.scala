package scrubjay.dataspace

import org.codehaus.jackson.annotate.JsonIgnore

case class Dimension(name: String, ordered: Boolean, continuous: Boolean)

case class DimensionSpace(dimensions: Array[Dimension]) {
  @JsonIgnore
  val map: Map[String, Dimension] = dimensions.map(d => (d.name, d)).toMap
  def findDimension(name: String): Option[Dimension] = map.get(name)
  def findDimensionOrDefault(name: String): Dimension = map.getOrElse(name, Dimension(name, ordered=false, continuous=false))
}

object DimensionSpace {
  def empty: DimensionSpace = DimensionSpace(Array())
}

