package scrubjay.dataset.original

import com.fasterxml.jackson.annotation.JsonSubTypes.Type
import com.fasterxml.jackson.annotation.{JsonIgnoreProperties, JsonSubTypes, JsonTypeInfo}
import scrubjay.dataset._

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
abstract class OriginalDatasetID extends DatasetID {
  override def dependencies: Seq[DatasetID] = Seq.empty
}


