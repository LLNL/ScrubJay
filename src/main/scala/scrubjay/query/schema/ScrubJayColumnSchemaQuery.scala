package scrubjay.query.schema

case class ScrubJayColumnSchemaQuery(domain: Option[Boolean] = None,
                                     name: Option[String] = None,
                                     dimension: Option[String] = None,
                                     units: Option[ScrubJayUnitsQuery] = None)

