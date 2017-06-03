package scrubjay.datasetid

import com.fasterxml.jackson.annotation.JsonIgnore

import scrubjay.util._

case class ScrubJayField(domain: Boolean,
                         name: String = "*",
                         dimension: String = "*",
                         units: String = "*",
                         aggregator: String = "average",
                         interpolator: String = "linear") {

  override def toString: String = {
    s"ScrubJayField(domain=$domain, name=$name, dimension=$dimension, units=$units, aggregator=$aggregator, interpolator=$interpolator)"
  }

  def matches(other: ScrubJayField): Boolean = {
    val domainMatches = domain == other.domain
    val dimensionMatches = dimension == other.dimension ||
      ScrubJayField.wildcards.contains(dimension) ||
      ScrubJayField.wildcards.contains(other.dimension)
    val unitsMatches = units == other.units ||
      ScrubJayField.wildcards.contains(units) ||
      ScrubJayField.wildcards.contains(other.units)
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
    domainType + ":" + dimension + ":" + units
  }

  def withGeneratedFieldName: ScrubJayField = {
    copy(name = generateFieldName)
  }
}

case class ScrubJaySchema(fields: Array[ScrubJayField]) {

  def apply(fieldName: String): ScrubJayField = map(fieldName)

  override def toString: String = {
    "ScrubJaySchema\n|--" + fields.mkString("\n|--")
  }

  def withGeneratedFieldNames: ScrubJaySchema = ScrubJaySchema(fields.map(_.withGeneratedFieldName))

  def fieldNames: Array[String] = fields.map(_.name)
  def dimensions: Array[String] = fields.map(_.dimension)
  def units: Array[String] = fields.map(_.units)

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
  def joinableFields(other: ScrubJaySchema): Array[(ScrubJayField, ScrubJayField)] = {
    domainFields.flatMap(domainField => {
      val otherMatch: Option[ScrubJayField] = other.domainFields.find(otherDomainField =>
        domainField.dimension == otherDomainField.dimension && domainField.units == otherDomainField.units)
      otherMatch.fold(None: Option[(ScrubJayField, ScrubJayField)])(f => Some(domainField, f))
    })
  }

  /**
    * When joined with "other", the resulting schema
    */
  def joinSchema(other: ScrubJaySchema): Option[ScrubJaySchema] = {
    if (joinableFields(other).nonEmpty)
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

object ScrubJayField {
  final val wildcards = Set("*", "any")
}
