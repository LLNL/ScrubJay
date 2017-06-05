package scrubjay.dataspace

import com.fasterxml.jackson.annotation.JsonIgnore

case class Dimension(name: String, ordered: Boolean, continuous: Boolean)

object Dimension {
  def unknown: Dimension = Dimension("UNKNOWN", false, false)
}

case class DimensionSpace(dimensions: Array[Dimension]) {
  @JsonIgnore
  val map: Map[String, Dimension] = dimensions.map(d => (d.name, d)).toMap
  def findDimension(name: String): Option[Dimension] = map.get(name)
  def findDimensionOrDefault(name: String): Dimension = map.getOrElse(name, Dimension(name, ordered=false, continuous=false))
}

object DimensionSpace {
  def unknown: DimensionSpace = DimensionSpace(Array(Dimension.unknown))
}

