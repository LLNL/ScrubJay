package scrubjay.query.schema

import scrubjay.datasetid.DatasetID
import scrubjay.query.constraintsolver.Combinatorics
import scrubjay.schema.ScrubJaySchema

case class ScrubJaySchemaQuery(columns: Set[ScrubJayColumnSchemaQuery]) {
  def matches(scrubJaySchema: ScrubJaySchema): Boolean = {
    columns.forall(queryColumn =>
      scrubJaySchema.columns.exists(schemaColumn =>
        queryColumn.matches(schemaColumn)))
  }

  def transformationPaths: Iterator[DatasetID => DatasetID] = {
    val pathsPerColumn = columns.map(column =>
      column.transformationPaths.toSeq).toSeq

    val combinations = Combinatorics.cartesian(pathsPerColumn).map(c =>
      c.reduce((a: DatasetID => DatasetID, b: DatasetID => DatasetID) =>
        (dsID: DatasetID) => a.apply(b.apply(dsID))))

    combinations
  }
}
