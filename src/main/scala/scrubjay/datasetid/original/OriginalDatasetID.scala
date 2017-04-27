package scrubjay.datasetid.original

import com.fasterxml.jackson.annotation.JsonSubTypes.Type
import com.fasterxml.jackson.annotation.{JsonIgnoreProperties, JsonSubTypes, JsonTypeInfo}
import scrubjay.datasetid._
import scrubjay.dataspace.DimensionSpace

@JsonIgnoreProperties(
  value = Array("valid") // not sure why this gets populated
)
@JsonTypeInfo(
  use = JsonTypeInfo.Id.NAME,
  include = JsonTypeInfo.As.PROPERTY,
  property = "type"
)
@JsonSubTypes(Array(
  new Type(value = classOf[CassandraDatasetID], name = "CassandraDatasetID"),
  new Type(value = classOf[LocalDatasetID], name = "LocalDatasetID"),
  new Type(value = classOf[CSVDatasetID], name = "CSVDatasetID"),
  new Type(value = classOf[CaliperKeyValueDatasetID], name = "CaliperKeyValueDatasetID")
))
abstract class OriginalDatasetID(scrubJaySchema: ScrubJaySchema) extends DatasetID {
  override def dependencies: Seq[DatasetID] = Seq.empty
  override def scrubJaySchema(dimensionSpace: DimensionSpace): ScrubJaySchema = scrubJaySchema
}


