package scrubjay.schema

case class ScrubJayColumnSchema(domain: Boolean,
                                name: String = UNKNOWN_STRING,
                                dimension: ScrubJayDimensionSchema = ScrubJayDimensionSchema(),
                                units: ScrubJayUnitsSchema = ScrubJayUnitsSchema()) {

  override def toString: String = {
    s"ScrubJayColumnSchema(domain=$domain, name=$name, dimension=$dimension, units=$units)"
  }

  def generateFieldName: String = {
    val domainType = if (domain) "domain" else "value"
    domainType + ":" + dimension.name + ":" + units.name
  }

  def withGeneratedColumnName: ScrubJayColumnSchema = {
    copy(name = generateFieldName)
  }
}

