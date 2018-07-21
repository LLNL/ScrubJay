package scrubjay.query.schema

import scrubjay.schema.ScrubJayDimensionSchema

case class ScrubJayDimensionQuery(name: Option[String] = None,
                                  ordered: Option[Boolean] = None,
                                  continuous: Option[Boolean] = None,
                                  subDimensions: Option[Seq[ScrubJayDimensionSchema]] = None)

