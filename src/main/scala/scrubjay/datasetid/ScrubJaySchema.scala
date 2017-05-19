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
}

case class ScrubJaySchema(fields: Array[ScrubJayField]) {
  def apply(fieldName: String): ScrubJayField = map(fieldName)

  def fieldNames: Array[String] = fields.map(_.name)
  def dimensions: Array[String] = fields.map(_.dimension)
  def units: Array[String] = fields.map(_.units)

  def domainDimensions: Array[String] = fields.filter(_.domain).map(_.dimension)
  def valueDimensions: Array[String] = fields.filterNot(_.domain).map(_.dimension)

  def containsDimensions(dimensions: Array[String]): Boolean = dimensions.forall(dimensions.contains)
  def containsDomainDimensions(dimensions: Array[String]): Boolean = dimensions.forall(domainDimensions.contains)
  def containsValueDimensions(dimensions: Array[String]): Boolean = dimensions.forall(valueDimensions.contains)

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

  @JsonIgnore
  val map: Map[String, ScrubJayField] = fields.map(field => (field.name, field)).toMap
}

object ScrubJayField {
  final val wildcards = Set("*", "any")
}
