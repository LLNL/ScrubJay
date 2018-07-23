package scrubjay.query.schema

case class ScrubJayColumnSchemaQuery(domain: Option[Boolean] = None,
                                     name: Option[String] = None,
                                     dimension: Option[ScrubJayDimensionSchemaQuery] = None,
                                     units: Option[ScrubJayUnitsQuery] = None) {
  def expand: Iterator[Set[ScrubJayColumnSchemaQuery]] = {
    if (dimension.isDefined)
      dimension.get.expand.map(expansion =>
        expansion.map(expandedDimension => copy(dimension = Some(expandedDimension)))
      )
    else
      Iterator.empty
  }
}

