package scrubjay.schema

import com.fasterxml.jackson.annotation.{JsonIgnore, JsonProperty}
import scrubjay.datasetid.DatasetID
import scrubjay.datasetid.transformation.ExplodeList
import scrubjay.query.constraintsolver.Combinatorics

case class ScrubJayDimensionSchema(name: String = UNKNOWN_STRING,
                                   ordered: Boolean = false,
                                   continuous: Boolean = false,
                                   subDimensions: Seq[ScrubJayDimensionSchema] = Seq.empty) {
  @JsonIgnore
  lazy val singleTransformation: DatasetID => DatasetID = {
    name match {
      case s if s.contains("list") =>
        (dsID: DatasetID) =>
          ExplodeList(dsID, subDimensions(0).name)
    }
  }

  def transformationPaths: Iterator[DatasetID => DatasetID] = {
    Combinatorics.decompositions(
      this,
      (q: ScrubJayDimensionSchema) => {
        q.subDimensions.nonEmpty
      },
      (q: ScrubJayDimensionSchema) => {
        q.subDimensions
      },
      (q: ScrubJayDimensionSchema) => {
        q.singleTransformation
      }
    )
  }
}
