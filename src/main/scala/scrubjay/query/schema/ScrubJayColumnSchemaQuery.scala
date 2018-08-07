package scrubjay.query.schema

import scrubjay.datasetid.DatasetID
import scrubjay.schema.ScrubJayColumnSchema

case class ScrubJayColumnSchemaQuery(domain: Option[Boolean] = None,
                                     name: Option[String] = None,
                                     dimension: Option[ScrubJayDimensionSchemaQuery] = None,
                                     units: Option[ScrubJayUnitsSchemaQuery] = None) {

  def matches(scrubJayColumnSchema: ScrubJayColumnSchema): Boolean = {
    val domainMatches = wildMatch(scrubJayColumnSchema.domain, domain)
    val dimensionMatches = dimension.isEmpty || dimension.get.matches(scrubJayColumnSchema.dimension)
    val unitsMatches = units.isEmpty || units.get.matches(scrubJayColumnSchema.units)
    domainMatches && dimensionMatches && unitsMatches
  }

  def transformationPaths: Iterator[DatasetID => DatasetID] = {
    dimension.getOrElse(ScrubJayDimensionSchemaQuery())
      .transformationPaths
  }
}
