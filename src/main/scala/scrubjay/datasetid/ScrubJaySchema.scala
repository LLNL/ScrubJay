package scrubjay.datasetid

import com.fasterxml.jackson.annotation.JsonIgnore

case class ScrubJayField(domain: Boolean,
                         name: String = "*",
                         dimension: String = "*",
                         units: String = "*") {
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

  def createComparableFields(compareDomain: Boolean = true,
                             compareName: Boolean = false,
                             compareDimension: Boolean = true,
                             compareUnits: Boolean = true): Set[ScrubJayField] = {
    // Generate fields with specified comparator
    fields.map(field =>
      new ScrubJayField(field.domain) {
        override def equals(obj: scala.Any): Boolean = obj match {
          case f: ScrubJayField => compareElements(f, compareDomain, compareName, compareDimension, compareUnits)
          case _ => false
        }
      }
    ).toSet
  }

  def commonFields(other: ScrubJaySchema,
                   compareDomain: Boolean = true,
                   compareName: Boolean = false,
                   compareDimension: Boolean = true,
                   compareUnits: Boolean = true): Array[ScrubJayField] = {
    createComparableFields(compareDomain, compareName, compareDimension, compareUnits)
      .intersect(other.createComparableFields(compareDomain, compareName, compareDimension, compareUnits))
      .toArray
  }

  override def equals(obj: scala.Any): Boolean = {
    obj match {
      case s: ScrubJaySchema => map == s.map
      case _ => false
    }
  }

  def satisfiesQuerySchema(other: ScrubJaySchema): Boolean = {
    // Every field in "other" has a match here
    other.fields.forall(otherField => fields.exists(_.matches(otherField)))
  }

  def joinSchema(other: ScrubJaySchema): Option[ScrubJaySchema] = {

    // Find common fields across domain, dimension, units
    val commonDomainDimensionUnits = commonFields(other,
      compareDomain = true, compareName = false, compareDimension = true, compareUnits = true)

    // If all domain fields are common, we have a join
    if (commonDomainDimensionUnits.length == domainDimensions.length)
      Some(ScrubJaySchema(commonDomainDimensionUnits ++ valueFields ++ other.valueFields))
    else
      None
  }

  @JsonIgnore
  val map: Map[String, ScrubJayField] = fields.map(field => (field.name, field)).toMap
}

object ScrubJayField {
  final val wildcards = Set("*", "any")
}
