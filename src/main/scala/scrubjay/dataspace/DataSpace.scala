// Copyright 2018 Lawrence Livermore National Security, LLC and other
// ScrubJay Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: (Apache-2.0 OR MIT)

package scrubjay.dataspace

import com.fasterxml.jackson.annotation.JsonIgnore
import scrubjay.datasetid.DatasetID
import scrubjay.schema.ScrubJayDimensionSchema
import scrubjay.util.writeStringToFile

import scala.io.Source

case class DataSpace(datasets: Array[DatasetID]) {
  def toJsonString: String = DataSpace.toJsonString(this)
  def writeToJsonFile(filename: String): Unit = DataSpace.writeToJsonFile(this, filename)

  @JsonIgnore
  val dimensions: Set[ScrubJayDimensionSchema] = datasets.flatMap(_.scrubJaySchema.columns.map(_.dimension)).toSet

  @JsonIgnore
  private val dimensionMap: Map[String, ScrubJayDimensionSchema] = dimensions.map{
    case d @ ScrubJayDimensionSchema(name, _, _, _) => (name, d)
  }.toMap

  def findDimension(name: String): Option[ScrubJayDimensionSchema] = dimensionMap.get(name)
  def findDimensionOrDefault(name: String): ScrubJayDimensionSchema = dimensionMap.getOrElse(name, ScrubJayDimensionSchema())
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

  def generateSkeletonFor(datasets: Array[DatasetID]): DataSpace = {
    DataSpace(datasets)
  }
}
