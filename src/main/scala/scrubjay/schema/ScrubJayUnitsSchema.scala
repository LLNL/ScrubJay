package scrubjay.schema

import scrubjay.query.schema.ScrubJayUnitsSchemaQuery

case class ScrubJayUnitsSchema(name: String = UNKNOWN_STRING,
                               elementType: String = "POINT",
                               aggregator: String = "maxcount",
                               interpolator: String = "nearest",
                               subUnits: Map[String, ScrubJayUnitsSchema] = Map.empty)
