package scrubjay.schema

import scrubjay.query.schema.ScrubJayUnitsQuery

case class ScrubJayUnitsSchema(name: String = UNKNOWN_STRING,
                               elementType: String = "POINT",
                               aggregator: String = "maxcount",
                               interpolator: String = "nearest",
                               subUnits: Map[String, ScrubJayUnitsSchema] = Map.empty) {
  def matchesQuery(query: ScrubJayUnitsQuery): Boolean = {
    val nameMatches = wildMatch(name, query.name)
    val elementTypeMatches = wildMatch(elementType, query.elementType)
    val aggregatorMatches = wildMatch(aggregator, query.aggregator)
    val interpolatorMatches = wildMatch(interpolator, query.interpolator)
    nameMatches && elementTypeMatches && aggregatorMatches && interpolatorMatches
  }
}
