package scrubjay.dataspace

import com.fasterxml.jackson.annotation.JsonIgnore
import scrubjay.schema.ScrubJayDimensionSchema

object DimensionSpace {
  val unknown: DimensionSpace = DimensionSpace()
}

case class DimensionSpace(dimensions: Array[ScrubJayDimensionSchema] = Array.empty) {
  @JsonIgnore
  val map: Map[String, ScrubJayDimensionSchema] = dimensions.map(d => (d.name, d)).toMap
  def findDimension(name: String): Option[ScrubJayDimensionSchema] = map.get(name)
  def findDimensionOrDefault(name: String): ScrubJayDimensionSchema = map.getOrElse(name, ScrubJayDimensionSchema(name))
}
