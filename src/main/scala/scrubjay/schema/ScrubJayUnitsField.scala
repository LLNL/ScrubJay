package scrubjay.schema

object ScrubJayUnitsField {

  val unknown: ScrubJayUnitsField = ScrubJayUnitsField(
    UNKNOWN_STRING,
    "POINT",
    "maxcount",
    "nearest",
    Map.empty
  )

  val any: ScrubJayUnitsFieldQuery = ScrubJayUnitsFieldQuery(
    WILDCARD_STRING,
    WILDCARD_STRING,
    WILDCARD_STRING,
    WILDCARD_STRING,
    Map.empty)
}

sealed abstract class ScrubJayUnitsFieldClass(name: String,
                                              elementType: String,
                                              aggregator: String,
                                              interpolator: String,
                                              subUnits: Map[String, ScrubJayUnitsField])
  extends Serializable

case class ScrubJayUnitsField(name: String = ScrubJayUnitsField.unknown.name,
                              elementType: String = ScrubJayUnitsField.unknown.elementType,
                              aggregator: String = ScrubJayUnitsField.unknown.aggregator,
                              interpolator: String = ScrubJayUnitsField.unknown.interpolator,
                              subUnits: Map[String, ScrubJayUnitsField] = ScrubJayUnitsField.unknown.subUnits)
  extends ScrubJayUnitsFieldClass(null, null, null, null, null) {
  def matchesQuery(query: ScrubJayUnitsFieldQuery): Boolean = {
    val nameMatches = wildMatch(name, query.name)
    val elementTypeMatches = wildMatch(elementType, query.elementType)
    val aggregatorMatches = wildMatch(aggregator, query.aggregator)
    val interpolatorMatches = wildMatch(interpolator, query.interpolator)
    nameMatches && elementTypeMatches && aggregatorMatches && interpolatorMatches
  }
}

case class ScrubJayUnitsFieldQuery(name: String = ScrubJayUnitsField.any.name,
                                   elementType: String = ScrubJayUnitsField.any.elementType,
                                   aggregator: String = ScrubJayUnitsField.any.aggregator,
                                   interpolator: String = ScrubJayUnitsField.any.interpolator,
                                   subUnits: Map[String, ScrubJayUnitsFieldQuery] = ScrubJayUnitsField.any.subUnits)
  extends ScrubJayUnitsFieldClass(null, null, null, null, null)
