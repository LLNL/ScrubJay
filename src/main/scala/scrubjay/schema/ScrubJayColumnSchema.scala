package scrubjay.schema

import scrubjay.query.schema.ScrubJayColumnSchemaQuery

case class ScrubJayColumnSchema(domain: Boolean,
                                name: String = UNKNOWN_STRING,
                                dimension: ScrubJayDimensionSchema = ScrubJayDimensionSchema(),
                                units: ScrubJayUnitsSchema = ScrubJayUnitsSchema()) {

  override def toString: String = {
    s"ScrubJayColumnSchema(domain=$domain, name=$name, dimension=$dimension, units=$units)"
  }

  def compareElements(other: ScrubJayColumnSchema,
                      compareDomain: Boolean = true,
                      compareName: Boolean = false,
                      compareDimension: Boolean = true,
                      compareUnits: Boolean = true): Boolean = {
    (!compareDomain      || domain    == other.domain)    &&
      (!compareName      || name      == other.name)      &&
      (!compareDimension || dimension == other.dimension) &&
      (!compareUnits     || units     == other.units)
  }

  def generateFieldName: String = {
    val domainType = if (domain) "domain" else "value"
    domainType + ":" + dimension.name + ":" + units.name
  }

  def withGeneratedColumnName: ScrubJayColumnSchema = {
    copy(name = generateFieldName)
  }

  def matchesQuery(query: ScrubJayColumnSchemaQuery): Boolean = {
    val domainMatches = wildMatch(domain, query.domain)
    val dimensionMatches = query.dimension.isEmpty || dimension.matchesQuery(query.dimension.get)
    val unitsMatches = query.units.isEmpty || units.matchesQuery(query.units.get)
    domainMatches && dimensionMatches && unitsMatches
  }
}

