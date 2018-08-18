package scrubjay.schema

case class ScrubJayDimensionSchema(name: String = UNKNOWN_STRING,
                                   ordered: Boolean = false,
                                   continuous: Boolean = false,
                                   subDimensions: Seq[ScrubJayDimensionSchema] = Seq.empty) {
}
