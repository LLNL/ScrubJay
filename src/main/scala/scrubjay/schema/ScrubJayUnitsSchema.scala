package scrubjay.schema

import scrubjay.datasetid.DatasetID
import scrubjay.datasetid.transformation.{ExplodeList, ExplodeRange}
import scrubjay.query.constraintsolver.Combinatorics

case class ScrubJayUnitsSchema(name: String = UNKNOWN_STRING,
                               elementType: String = "POINT",
                               aggregator: String = "maxcount",
                               interpolator: String = "nearest",
                               subUnits: Map[String, ScrubJayUnitsSchema] = Map.empty) {

  def singleTransformation(associatedColumn: String): DatasetID => DatasetID = {
    elementType match {
      case "MULTIPOINT" =>
        (dsID: DatasetID) =>
          ExplodeList(dsID, associatedColumn)
      case "RANGE" =>
        (dsID: DatasetID) =>
          ExplodeRange(dsID, associatedColumn)
    }
  }

  def transformationPaths(associatedColumn: String): Iterator[DatasetID => DatasetID] = {
    Combinatorics.decompositions(
      this,
      (q: ScrubJayUnitsSchema) => {
        q.subUnits != null && q.subUnits.nonEmpty
      },
      (q: ScrubJayUnitsSchema) => {
        if (q.subUnits != null) q.subUnits.values.toSeq else Seq()
      },
      (q: ScrubJayUnitsSchema) => {
        q.singleTransformation(associatedColumn)
      }
    )
  }
}
