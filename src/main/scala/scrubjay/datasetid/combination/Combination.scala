package scrubjay.datasetid.combination

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
  new Type(value = classOf[NaturalJoin], name = "NaturalJoin"),
  new Type(value = classOf[InterpolationJoin], name = "InterpolationJoin")
))
abstract class Combination extends DatasetID {
  val dsID1: DatasetID
  val dsID2: DatasetID
  override def dependencies: Seq[DatasetID] = Seq(dsID1, dsID2)
}
