package scrubjay.dataspace

import com.fasterxml.jackson.annotation.JsonIgnore
import scrubjay.datasetid.DatasetID
import scrubjay.schema.ScrubJayDimensionSchema
import scrubjay.util.writeStringToFile

import scala.io.Source

case class DataSpace(dimensionSpace: DimensionSpace, datasets: Array[DatasetID]) {
  def toJsonString: String = DataSpace.toJsonString(this)
  def writeToJsonFile(filename: String): Unit = DataSpace.writeToJsonFile(this, filename)

  @JsonIgnore
  private val dimensionMap: Map[String, ScrubJayDimensionSchema] = dimensionSpace.dimensions.map{
    case d @ ScrubJayDimensionSchema(name, _, _, _) => (name, d)
  }.toMap

  def dimension(name: String): ScrubJayDimensionSchema = dimensionMap(name)
}

object DataSpace {

  def toJsonString(ds: DataSpace): String = {
    DatasetID.objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(ds)
  }

  def fromJsonString(json: String): DataSpace = {
    // TODO: check if all dimensions are defined
    DatasetID.objectMapper.readValue[DataSpace](json, classOf[DataSpace])
  }

  def writeToJsonFile(ds: DataSpace, filename: String): Unit = {
    writeStringToFile(toJsonString(ds), filename)
  }

  def fromJsonFile(filename: String): DataSpace = {
    val fileContents = Source.fromFile(filename).getLines.mkString("\n")
    fromJsonString(fileContents)
  }

  def generateSkeletonFor(datasets: Array[DatasetID]): DataSpace = {
    DataSpace(DimensionSpace.unknown, datasets)
  }
}
