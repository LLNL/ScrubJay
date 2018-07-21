package scrubjay.datasetid.transformation

import com.fasterxml.jackson.annotation.JsonSubTypes.Type
import com.fasterxml.jackson.annotation.{JsonIgnoreProperties, JsonSubTypes, JsonTypeInfo}
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
  new Type(value = classOf[DeriveRate], name = "DeriveRate"),
  new Type(value = classOf[ExplodeList], name = "ExplodeDiscreteRange"),
  new Type(value = classOf[ExplodeRange], name = "ExplodeContinuousRange")
))
abstract class Transformation extends DatasetID {
  val dsID: DatasetID
  override def dependencies: Seq[DatasetID] = Seq(dsID)
}
