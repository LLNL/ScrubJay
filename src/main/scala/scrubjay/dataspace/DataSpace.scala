package scrubjay.dataspace

import scrubjay.datasetid.DatasetID
import scrubjay.util.writeStringToFile

import scala.io.Source

case class DimensionSpace(name: String, ordered: Boolean, continuous: Boolean)

case class DataSpace(dimensions: Array[DimensionSpace], datasets: Array[DatasetID]) {
  def toJsonString: String = DataSpace.toJsonString(this)
  def writeToJsonFile(filename: String): Unit = DataSpace.writeToJsonFile(this, filename)
}

object DataSpace {

  def toJsonString(ds: DataSpace): String = {
    DatasetID.objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(ds)
  }

  def fromJsonString(json: String): DataSpace = {
    DatasetID.objectMapper.readValue[DataSpace](json, classOf[DataSpace])
  }

  def writeToJsonFile(ds: DataSpace, filename: String): Unit = {
    writeStringToFile(toJsonString(ds), filename)
  }

  def fromJsonFile(filename: String): DataSpace = {
    val fileContents = Source.fromFile(filename).getLines.mkString("\n")
    fromJsonString(fileContents)
  }
}
