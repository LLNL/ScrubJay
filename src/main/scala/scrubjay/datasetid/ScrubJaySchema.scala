package scrubjay.datasetid

import com.fasterxml.jackson.annotation.JsonIgnore

import scrubjay.util._

case class ScrubJayUnitsField(name: String,
                              elementType: String,
                              aggregator: String,
                              interpolator: String,
                              subUnits: Map[String, ScrubJayUnitsField]) {
  def matches(other: ScrubJayUnitsField): Boolean = {
    val nameMatches = wildMatch(name, other.name)
    val elementTypeMatches = wildMatch(elementType, other.elementType)
    val aggregatorMatches = wildMatch(aggregator, other.aggregator)
    val interpolatorMatches = wildMatch(interpolator, other.interpolator)
    nameMatches && elementTypeMatches && aggregatorMatches && interpolatorMatches
  }
}

object ScrubJayUnitsField {
  val any: ScrubJayUnitsField = ScrubJayUnitsField(
    WILDCARD_STRING,
    WILDCARD_STRING,
    WILDCARD_STRING,
    WILDCARD_STRING,
    Map.empty)
}

case class ScrubJayField(domain: Boolean,
                         name: String = WILDCARD_STRING,
                         dimension: String = WILDCARD_STRING,
                         units: ScrubJayUnitsField = ScrubJayUnitsField.any) {

  override def toString: String = {
    s"ScrubJayField(domain=$domain, name=$name, dimension=$dimension, units=$units)"
  }

  def matches(other: ScrubJayField): Boolean = {
    val domainMatches = domain == other.domain
    val dimensionMatches = wildMatch(dimension, other.dimension)
    val unitsMatches = units.matches(other.units)
    domainMatches && dimensionMatches && unitsMatches
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
}

case class ScrubJaySchema(fields: Array[ScrubJayField]) {

  def getField(fieldName: String): ScrubJayField = map(fieldName)

  override def toString: String = {
    "ScrubJaySchema\n|--" + fields.mkString("\n|--")
  }

  def withGeneratedFieldNames: ScrubJaySchema = ScrubJaySchema(fields.map(_.withGeneratedFieldName))

  def fieldNames: Array[String] = fields.map(_.name)
  def dimensions: Array[String] = fields.map(_.dimension)
  def units: Array[ScrubJayUnitsField] = fields.map(_.units)

  def domainFields: Array[ScrubJayField] = fields.filter(_.domain)
  def valueFields: Array[ScrubJayField] = fields.filterNot(_.domain)

  def domainDimensions: Array[String] = domainFields.map(_.dimension)
  def valueDimensions: Array[String] = valueFields.map(_.dimension)

  def containsDimensions(dimensions: Array[String]): Boolean = dimensions.forall(dimensions.contains)
  def containsDomainDimensions(dimensions: Array[String]): Boolean = dimensions.forall(domainDimensions.contains)
  def containsValueDimensions(dimensions: Array[String]): Boolean = dimensions.forall(valueDimensions.contains)

  override def equals(obj: scala.Any): Boolean = {
    obj match {
      case s: ScrubJaySchema => map == s.map
      case _ => false
    }
  }

  /**
    * This schema satisfies a target schema if every field in the target has a match here
    */
  def satisfiesQuerySchema(target: ScrubJaySchema): Boolean = {
    target.fields.forall(targetField => fields.exists(_.matches(targetField)))
  }

  /**
    * Joinable fields are domain fields with dimension and units in common
    */
  def joinableFields(other: ScrubJaySchema, testUnits: Boolean = true): Array[(ScrubJayField, ScrubJayField)] = {
    domainFields.flatMap(domainField => {
      val otherMatch: Option[ScrubJayField] = other.domainFields.find(otherDomainField =>
        domainField.dimension == otherDomainField.dimension && (!testUnits || domainField.units == otherDomainField.units))
      otherMatch.fold(None: Option[(ScrubJayField, ScrubJayField)])(f => Some(domainField, f))
    })
  }

  /**
    * When joined with "other", the resulting schema
    */
  def joinSchema(other: ScrubJaySchema, testUnits: Boolean = true): Option[ScrubJaySchema] = {
    if (joinableFields(other, testUnits).nonEmpty)
      Some(ScrubJaySchema(
        domainFields.toSet.union(other.domainFields.toSet).toArray
          ++ valueFields
          ++ other.valueFields))
    else
      None
  }

  @JsonIgnore
  val map: Map[String, ScrubJayField] = fields.map(field => (field.name, field)).toMap
}
