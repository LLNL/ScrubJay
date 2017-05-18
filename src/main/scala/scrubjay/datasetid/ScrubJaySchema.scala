package scrubjay.datasetid

import com.fasterxml.jackson.annotation.JsonIgnore

case class ScrubJayField(domain: Boolean,
                         name: String = "*",
                         dimension: String = "*",
                         units: String = "*") {
  def matches(other: ScrubJayField): Boolean = {
    domain == other.domain &&
      Array(dimension, units).zip(Array(other.dimension, other.units))
        .forall{
          case (left, right) => {
            // Either one side is a wildcard, or they're exactly the same, otherwise no match
            Set(left, right).intersect(ScrubJayField.wildcards).nonEmpty || left == right
          }
        }
  }
}

case class ScrubJaySchema(fields: Array[ScrubJayField]) {
  def apply(fieldName: String): ScrubJayField = map(fieldName)

  def fieldNames: Array[String] = fields.map(_.name)
  def dimensions: Array[String] = fields.map(_.dimension)
  def units: Array[String] = fields.map(_.units)

  override def equals(obj: scala.Any): Boolean = {
    obj match {
      case s: ScrubJaySchema => map == s.map
      case _ => false
    }
  }

  def containsMatchesFor(other: ScrubJaySchema): Boolean = {
    // Every field in "other" has a match here
    other.fields.forall(otherField => fields.exists(_.matches(otherField)))
  }

  @JsonIgnore
  val map: Map[String, ScrubJayField] = fields.map(field => (field.name, field)).toMap
}

object ScrubJayField {
  final val wildcards = Set("*", "any")
}
