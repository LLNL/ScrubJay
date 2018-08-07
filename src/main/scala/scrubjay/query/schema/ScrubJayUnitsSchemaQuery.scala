package scrubjay.query.schema

import scrubjay.schema.ScrubJayUnitsSchema

case class ScrubJayUnitsSchemaQuery(name: Option[String] = None,
                                    elementType: Option[String] = None,
                                    aggregator: Option[String] = None,
                                    interpolator: Option[String] = None,
                                    subUnits: Option[Map[String, ScrubJayUnitsSchemaQuery]] = None) {
  def matches(scrubJayUnitsSchema: ScrubJayUnitsSchema): Boolean = {
    val nameMatches = wildMatch(scrubJayUnitsSchema.name, name)
    val elementTypeMatches = wildMatch(scrubJayUnitsSchema.elementType, elementType)
    val aggregatorMatches = wildMatch(scrubJayUnitsSchema.aggregator, aggregator)
    val interpolatorMatches = wildMatch(scrubJayUnitsSchema.interpolator, interpolator)
    nameMatches && elementTypeMatches && aggregatorMatches && interpolatorMatches
  }
}

