package scrubjay.schema

import scrubjay.datasetid.transformation.Transformation
import scrubjay.query.schema.ScrubJayDimensionSchemaQuery

case class ScrubJayDimensionSchema(name: String = UNKNOWN_STRING,
                                   ordered: Boolean = false,
                                   continuous: Boolean = false,
                                   subDimensions: Seq[ScrubJayDimensionSchema] = Seq.empty) {
  def matchesQuery(query: ScrubJayDimensionSchemaQuery): Boolean = {
    val nameMatches = wildMatch(name, query.name)
    val orderedMatches = wildMatch(ordered, query.ordered)
    val continuousMatches = wildMatch(continuous, query.continuous)
    val subDimensionsMatch = query.subDimensions.isEmpty || subDimensions.forall(subDimension =>
      query.subDimensions.get.exists(q => subDimension.matchesQuery(q)))
    nameMatches && orderedMatches && continuousMatches && subDimensionsMatch
  }

  def derivationPathToQuery(query: ScrubJayDimensionSchemaQuery): Iterator[Seq[Transformation]] = {

    // if matchesQuery, add empty Seq as first result
    val noDerivationMatches: Iterator[Seq[Transformation]] = if (matchesQuery(query)) Iterator(Seq.empty) else Iterator.empty

    // check if any expansion of the query matches this
    query.expand.map((expandedQuery) => ???)
    ???
  }
}

