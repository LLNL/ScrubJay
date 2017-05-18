package scrubjay.datasetid

import com.fasterxml.jackson.annotation.{JsonIgnore, JsonTypeInfo}

case class ScrubJayField(name: String, dimension: String, domain: Boolean = false)

case class ScrubJaySchema(fields: Array[ScrubJayField]) {
  def apply(fieldName: String): ScrubJayField = map(fieldName)

  def fieldNames: Array[String] = fields.map(_.name)
  def dimensions: Array[String] = fields.map(_.dimension)

  override def equals(obj: scala.Any): Boolean = {
    obj match {
      case s: ScrubJaySchema => map == s.map
      case _ => false
    }
  }

  @JsonIgnore
  val map: Map[String, ScrubJayField] = fields.map(field => (field.name, field)).toMap
}
