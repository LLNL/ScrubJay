package scrubjay.dataspace

import scrubjay.datasetid.DatasetID
import scrubjay.util.writeStringToFile

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.module.SimpleModule
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper

import scala.io.Source

case class DimensionSpace(name: String, ordered: Boolean, continuous: Boolean)

case class DataSpace(dimensions: Array[DimensionSpace], datasets: Array[DatasetID]) {
  def toJsonString: String = DataSpace.toJsonString(this)
  def writeToJsonFile(filename: String): Unit = DataSpace.writeToJsonFile(this, filename)
}

object DataSpace {

  private val objectMapper: ObjectMapper with ScalaObjectMapper = {
    val structTypeModule: SimpleModule = new SimpleModule()
    val m = new ObjectMapper with ScalaObjectMapper
    m.registerModule(DefaultScalaModule)
    m.registerModule(structTypeModule)
    m
  }

  def toJsonString(ds: DataSpace): String = {
    objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(ds)
  }

  def fromJsonString(json: String): DataSpace = {
    objectMapper.readValue[DataSpace](json, classOf[DataSpace])
  }

  def writeToJsonFile(ds: DataSpace, filename: String): Unit = {
    writeStringToFile(toJsonString(ds), filename)
  }

  def fromJsonFile(filename: String): DataSpace = {
    val fileContents = Source.fromFile(filename).getLines.mkString("\n")
    fromJsonString(fileContents)
  }
}
