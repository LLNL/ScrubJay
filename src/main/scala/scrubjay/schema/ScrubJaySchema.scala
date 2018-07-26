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

  // Possible TODO: make getField return an Option
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

  @JsonIgnore
  private val dimensionMap: Map[String, ScrubJayDimensionSchema] = dimensions.map{
    case d @ ScrubJayDimensionSchema(name, _, _, _) => (name, d)
  }.toMap

  def findDimension(name: String): Option[ScrubJayDimensionSchema] = dimensionMap.get(name)
  def findDimensionOrDefault(name: String): ScrubJayDimensionSchema = dimensionMap.getOrElse(name, ScrubJayDimensionSchema())

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
    val domainPairs = domainFields.flatMap(domainField =>
      other.domainFields.map(otherDomainField => (domainField, otherDomainField)))

    domainPairs.filter(domainPair =>
      domainPair._1.dimension == domainPair._2.dimension
      && (!testUnits || domainPair._1.units == domainPair._2.units)
    )
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

