package scrubjay.schema

import scrubjay.query.schema.ScrubJayDimensionSchemaQuery

case class ScrubJayDimensionSchema(name: String = UNKNOWN_STRING,
                                   ordered: Boolean = false,
                                   continuous: Boolean = false,
                                   subDimensions: Seq[ScrubJayDimensionSchema] = Seq.empty) {
  def matchesQuery(query: ScrubJayDimensionSchemaQuery): Boolean = {
    val nameMatches = wildMatch(name, query.name)
    val orderedMatches = wildMatch(ordered, query.ordered)
    val continuousMatches = wildMatch(continuous, query.continuous)
    val subDimensionsMatch = subDimensions.isEmpty || subDimensions.forall(subDimension =>
      query.subDimensions.get.exists(q => subDimension.matchesQuery(q)))
    nameMatches && orderedMatches && continuousMatches && subDimensionsMatch
  }
}

