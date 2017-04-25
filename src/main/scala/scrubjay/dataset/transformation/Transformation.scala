package scrubjay.dataset.transformation

import com.fasterxml.jackson.annotation.JsonSubTypes.Type
import com.fasterxml.jackson.annotation.{JsonIgnoreProperties, JsonSubTypes, JsonTypeInfo}
import scrubjay.dataset.DatasetID

@JsonIgnoreProperties(
  value = Array("valid") // not sure why this gets populated
)
@JsonTypeInfo(
  use = JsonTypeInfo.Id.NAME,
  include = JsonTypeInfo.As.PROPERTY,
  property = "type"
)
@JsonSubTypes(Array(
  new Type(value = classOf[ExplodeDiscreteRange], name = "ExplodeDiscreteRange")
))
abstract class Transformation extends DatasetID {
  val dsID: DatasetID
  override def dependencies: Seq[DatasetID] = Seq(dsID)
}
