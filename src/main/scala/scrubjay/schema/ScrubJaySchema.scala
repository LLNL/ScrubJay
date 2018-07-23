package scrubjay.schema

import com.fasterxml.jackson.annotation.JsonIgnore
import scrubjay.query.schema.ScrubJaySchemaQuery

object ScrubJaySchema {
  def unknown(sparkSchema: SparkSchema): ScrubJaySchema = {
    ScrubJaySchema(sparkSchema.fieldNames.toSet.map((name: String) =>
      ScrubJayColumnSchema(domain = false, name)))
  }
}

case class ScrubJaySchema(fields: Set[ScrubJayColumnSchema]) {

  def getField(fieldName: String): ScrubJayColumnSchema = map(fieldName)

  override def toString: String = {
    "ScrubJaySchema\n|--" + fields.mkString("\n|--")
  }

  def withGeneratedColumnNames: ScrubJaySchema = ScrubJaySchema(fields.map(_.withGeneratedColumnName))

  def columnNames: Set[String] = fields.map(_.name)
  def dimensions: Set[ScrubJayDimensionSchema] = fields.map(_.dimension)
  def units: Set[ScrubJayUnitsSchema] = fields.map(_.units)

  def domainFields: Set[ScrubJayColumnSchema] = fields.filter(_.domain)
  def valueFields: Set[ScrubJayColumnSchema] = fields.filterNot(_.domain)

  def domainDimensions: Set[ScrubJayDimensionSchema] = domainFields.map(_.dimension)
  def valueDimensions: Set[ScrubJayDimensionSchema] = valueFields.map(_.dimension)

  def containsDimensions(dimensions: Set[String]): Boolean = dimensions.forall(dimensions.contains)
  def containsDomainDimensions(dimensions: Set[ScrubJayDimensionSchema]): Boolean = dimensions.forall(domainDimensions.contains)
  def containsValueDimensions(dimensions: Set[ScrubJayDimensionSchema]): Boolean = dimensions.forall(valueDimensions.contains)

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
    query.columns.forall(targetField => fields.exists(_.matchesQuery(targetField)))
  }

  /**
    * Joinable columns are domain columns with dimension and units in common
    */
  def joinableFields(other: ScrubJaySchema, testUnits: Boolean = true): Set[(ScrubJayColumnSchema, ScrubJayColumnSchema)] = {
    domainFields.flatMap(domainField => {
      val otherMatch: Option[ScrubJayColumnSchema] = other.domainFields.find(otherDomainField =>
        domainField.dimension == otherDomainField.dimension && (!testUnits || domainField.units == otherDomainField.units))
      otherMatch.fold(None: Option[(ScrubJayColumnSchema, ScrubJayColumnSchema)])(f => Some(domainField, f))
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
  val map: Map[String, ScrubJayColumnSchema] = fields.map(field => (field.name, field)).toMap
}

