package scrubjay.query.schema

case class ScrubJayUnitsQuery(name: Option[String] = None,
                              elementType: Option[String] = None,
                              aggregator: Option[String] = None,
                              interpolator: Option[String] = None,
                              subUnits: Option[Map[String, ScrubJayUnitsQuery]] = None)

