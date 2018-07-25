package scrubjay.datasetid.original

import com.fasterxml.jackson.annotation.JsonSubTypes.Type
import com.fasterxml.jackson.annotation.{JsonIgnore, JsonIgnoreProperties, JsonSubTypes, JsonTypeInfo}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.scrubjayunits.ScrubJayDFLoader
import scrubjay.datasetid._
import scrubjay.dataspace.DimensionSpace
import scrubjay.schema.ScrubJaySchema

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
abstract class OriginalDatasetID(name: String, originalScrubJaySchema: ScrubJaySchema) extends DatasetID(name) {

  @JsonIgnore
  lazy val spark: SparkSession = SparkSession.builder().getOrCreate()

  def originalDF: DataFrame

  override def realize: DataFrame = {
    ScrubJayDFLoader.load(originalDF, originalScrubJaySchema)
  }

  override def dependencies: Seq[DatasetID] = Seq.empty
  override def scrubJaySchema: ScrubJaySchema = originalScrubJaySchema.withGeneratedColumnNames
}

