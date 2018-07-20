package scrubjay.schema

object ScrubJayField {
  val unknown = ScrubJayField(false, UNKNOWN_STRING, UNKNOWN_STRING, ScrubJayUnitsField.unknown)
  val any = ScrubJayFieldQuery(false, WILDCARD_STRING, WILDCARD_STRING, ScrubJayUnitsField.any)
}

sealed abstract class ScrubJayFieldClass(domain: Boolean,
                                         name: String,
                                         dimension: String)
  extends Serializable

case class ScrubJayField(domain: Boolean,
                         name: String = ScrubJayField.unknown.name,
                         dimension: String = ScrubJayField.unknown.dimension,
                         units: ScrubJayUnitsField = ScrubJayField.unknown.units)
  extends ScrubJayFieldClass(domain, name, dimension) {

  override def toString: String = {
    s"ScrubJayField(domain=$domain, name=$name, dimension=$dimension, units=$units)"
  }

  def compareElements(other: ScrubJayField,
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
    domainType + ":" + dimension + ":" + units.name
  }

  def withGeneratedFieldName: ScrubJayField = {
    copy(name = generateFieldName)
  }

  def matchesQuery(query: ScrubJayFieldQuery): Boolean = {
    val domainMatches = domain == query.domain
    val dimensionMatches = wildMatch(dimension, query.dimension)
    val unitsMatches = units.matchesQuery(query.units)
    domainMatches && dimensionMatches && unitsMatches
  }
}

case class ScrubJayFieldQuery(domain: Boolean,
                         name: String = ScrubJayField.any.name,
                         dimension: String = ScrubJayField.any.dimension,
                         units: ScrubJayUnitsFieldQuery = ScrubJayField.any.units)
  extends ScrubJayFieldClass(domain, name, dimension) {

}
