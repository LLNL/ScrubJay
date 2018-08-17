package scrubjay.datasetid.transformation

import com.fasterxml.jackson.annotation.JsonSubTypes.Type
import com.fasterxml.jackson.annotation.{JsonIgnoreProperties, JsonSubTypes, JsonTypeInfo}
import scrubjay.datasetid.DatasetID
import scrubjay.query.schema.{ScrubJayColumnSchemaQuery, ScrubJaySchemaQuery}

@JsonIgnoreProperties(
  Array(
    "valid",
    "scrubJaySchema",
    "columnDependencies"
  )
)
@JsonTypeInfo(
  use = JsonTypeInfo.Id.NAME,
  include = JsonTypeInfo.As.PROPERTY,
  property = "type"
)
@JsonSubTypes(Array(
  new Type(value = classOf[DeriveRate], name = "DeriveRate"),
  new Type(value = classOf[ExplodeList], name = "ExplodeList"),
  new Type(value = classOf[ExplodeRange], name = "ExplodeRange")
))
abstract class Transformation(name: String) extends DatasetID(name) {
  val dsID: DatasetID
  val columnDependencies: Set[ScrubJayColumnSchemaQuery]

  override def validFn: Boolean = dsID.valid &&
    ScrubJaySchemaQuery(columnDependencies).matches(dsID.scrubJaySchema)
}
