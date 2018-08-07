package scrubjay.query.schema

import scrubjay.datasetid.DatasetID
import scrubjay.datasetid.transformation.DeriveRate
import scrubjay.query.constraintsolver.Combinatorics
import scrubjay.schema.ScrubJayDimensionSchema

case class ScrubJayDimensionSchemaQuery(name: Option[String] = None,
                                        ordered: Option[Boolean] = None,
                                        continuous: Option[Boolean] = None,
                                        subDimensions: Option[Seq[ScrubJayDimensionSchemaQuery]] = None) {

  def matches(scrubJayDimensionSchema: ScrubJayDimensionSchema): Boolean = {
    val nameMatches = wildMatch(scrubJayDimensionSchema.name, name)
    val orderedMatches = wildMatch(scrubJayDimensionSchema.ordered, ordered)
    val continuousMatches = wildMatch(scrubJayDimensionSchema.continuous, continuous)
    val subDimensionsMatch = subDimensions.isEmpty || scrubJayDimensionSchema.subDimensions.forall(subDimension =>
      subDimensions.get.exists(q => q.matches(subDimension)))
    nameMatches && orderedMatches && continuousMatches && subDimensionsMatch
  }

  def transformationPaths: Iterator[DatasetID => DatasetID] = {

    val combinations: Iterator[DatasetID => DatasetID] = {
      if (name.isDefined && subDimensions.isDefined && subDimensions.get.nonEmpty) {
        val subDimensionsSeq = subDimensions.getOrElse(Seq[ScrubJayDimensionSchemaQuery]())

        val singleTransformation: DatasetID => DatasetID = name.get match {
          case "rate" =>
            (dsID: DatasetID) =>
              DeriveRate(dsID, subDimensionsSeq(0).name.get, subDimensionsSeq(1).name.get, 10)
        }

        // Expand all subdimensions recursively
        val recursiveCase: Seq[Seq[DatasetID => DatasetID]] =
          subDimensionsSeq.map(s => s.transformationPaths.toSeq)

        // Cross product of expansions of subunits (ways to do first * ways to do second * ...)
        val combinations: Iterator[DatasetID => DatasetID] =
          Combinatorics.cartesian(recursiveCase).map(c =>
            c.reduce((a: DatasetID => DatasetID, b: DatasetID => DatasetID) =>
              (dsID: DatasetID) => singleTransformation.apply(a.apply(b.apply(dsID)))))

        combinations
      } else {
        Iterator.empty
      }
    }

    // Base + Recursive
    Iterator((dsID: DatasetID) => dsID) ++ combinations
  }
}
