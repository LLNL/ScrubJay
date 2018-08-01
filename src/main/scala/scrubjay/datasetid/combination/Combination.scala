package scrubjay.datasetid.combination

import com.fasterxml.jackson.annotation.JsonSubTypes.Type
import com.fasterxml.jackson.annotation.{JsonIgnore, JsonIgnoreProperties, JsonSubTypes, JsonTypeInfo}
import scrubjay.datasetid.DatasetID

@JsonIgnoreProperties(
  value = Array("valid") // not sure why this gets populated
)
@JsonTypeInfo(
  use = JsonTypeInfo.Id.NAME,
  include = JsonTypeInfo.As.PROPERTY,
  property = "type"
)
@JsonSubTypes(Array(
  new Type(value = classOf[NaturalJoin], name = "NaturalJoin"),
  new Type(value = classOf[InterpolationJoin], name = "InterpolationJoin")
))
abstract class Combination(name: String) extends DatasetID(name) {
  val dsID1: DatasetID
  val dsID2: DatasetID
}
