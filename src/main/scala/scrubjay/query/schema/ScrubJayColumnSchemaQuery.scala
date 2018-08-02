package scrubjay.query.schema

case class ScrubJayColumnSchemaQuery(domain: Option[Boolean] = None,
                                     name: Option[String] = None,
                                     dimension: Option[ScrubJayDimensionSchemaQuery] = None,
                                     units: Option[ScrubJayUnitsQuery] = None) {
}
