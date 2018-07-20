package scrubjay.schema

import com.fasterxml.jackson.annotation.JsonIgnore
import scrubjay.dataspace.Dimension

case class ScrubJaySchema(fields: Set[ScrubJayField]) {

  def getField(fieldName: String): ScrubJayField = map(fieldName)

  override def toString: String = {
    "ScrubJaySchema\n|--" + fields.mkString("\n|--")
  }

  def withGeneratedFieldNames: ScrubJaySchema = ScrubJaySchema(fields.map(_.withGeneratedFieldName))

  def fieldNames: Set[String] = fields.map(_.name)
  def dimensions: Set[String] = fields.map(_.dimension)
  def units: Set[ScrubJayUnitsField] = fields.map(_.units)

  def domainFields: Set[ScrubJayField] = fields.filter(_.domain)
  def valueFields: Set[ScrubJayField] = fields.filterNot(_.domain)

  def domainDimensions: Set[String] = domainFields.map(_.dimension)
  def valueDimensions: Set[String] = valueFields.map(_.dimension)

  def containsDimensions(dimensions: Set[String]): Boolean = dimensions.forall(dimensions.contains)
  def containsDomainDimensions(dimensions: Set[String]): Boolean = dimensions.forall(domainDimensions.contains)
  def containsValueDimensions(dimensions: Set[String]): Boolean = dimensions.forall(valueDimensions.contains)

  override def equals(obj: scala.Any): Boolean = {
    obj match {
      case s: ScrubJaySchema => map == s.map
      case _ => false
    }
  }

  /**
    * This schema satisfies a target schema if every field in the target has a match here
    */
  def matchesQuery(query: ScrubJaySchemaQuery): Boolean = {
    query.fields.forall(targetField => fields.exists(_.matchesQuery(targetField)))
  }

  /**
    * Joinable fields are domain fields with dimension and units in common
    */
  def joinableFields(other: ScrubJaySchema, testUnits: Boolean = true): Set[(ScrubJayField, ScrubJayField)] = {
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
        domainFields.union(other.domainFields)
          ++ valueFields
          ++ other.valueFields))
    else
      None
  }

  @JsonIgnore
  val map: Map[String, ScrubJayField] = fields.map(field => (field.name, field)).toMap
}

case class ScrubJaySchemaQuery(fields: Set[ScrubJayFieldQuery]) {
}

object ScrubJaySchema {
  def unknown(sparkSchema: SparkSchema): ScrubJaySchema = {
    ScrubJaySchema(sparkSchema.fieldNames.toSet.map((name: String) =>
      ScrubJayField(domain = false, name, Dimension.unknown.name, ScrubJayUnitsField.unknown)))
  }
}
