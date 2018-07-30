package scrubjay.query.schema

import scrubjay.query.constraintsolver.Combinatorics

case class ScrubJayDimensionSchemaQuery(name: Option[String] = None,
                                        ordered: Option[Boolean] = None,
                                        continuous: Option[Boolean] = None,
                                        subDimensions: Option[Seq[ScrubJayDimensionSchemaQuery]] = None) {

  def expand: Iterator[Set[ScrubJayDimensionSchemaQuery]] = {
    val subDimensionsSeq = subDimensions.getOrElse(Seq[ScrubJayDimensionSchemaQuery]())

    // Expand all subdimensions recursively
    val recursiveCase: Seq[Seq[Set[ScrubJayDimensionSchemaQuery]]] =
      subDimensionsSeq.map(s => s.expand.toSeq)

    // Cross product of expansions of subunits (ways to do first * ways to do second * ...)
    val combinations: Iterator[Set[ScrubJayDimensionSchemaQuery]] =
      Combinatorics.cartesian(recursiveCase).map(c => c.reduce((a, b) => a union b))

    Iterator(Set(this)) ++ combinations
  }
}
