package scrubjay.query.schema

import scrubjay.query.constraintsolver.Combinatorics

case class ScrubJayDimensionSchemaQuery(name: Option[String] = None,
                                        ordered: Option[Boolean] = None,
                                        continuous: Option[Boolean] = None,
                                        subDimensions: Option[Seq[ScrubJayDimensionSchemaQuery]] = None) {

  def expand: Iterator[(Seq[String], Set[ScrubJayDimensionSchemaQuery])] = {

    val combinations: Iterator[(Seq[String], Set[ScrubJayDimensionSchemaQuery])] = if (name.isDefined) {
      val subDimensionsSeq = subDimensions.getOrElse(Seq[ScrubJayDimensionSchemaQuery]())

      // Expand all subdimensions recursively
      val recursiveCase: Seq[Seq[(Seq[String], Set[ScrubJayDimensionSchemaQuery])]] =
        subDimensionsSeq.map(s => s.expand.toSeq)

      // Cross product of expansions of subunits (ways to do first * ways to do second * ...)
      val combinations: Iterator[(Seq[String], Set[ScrubJayDimensionSchemaQuery])] =
        Combinatorics.cartesian(recursiveCase).map(c => c.reduce((a, b) => (name.get +: (a._1 ++ b._1), a._2 union b._2)))

      combinations
    } else {
      Iterator.empty
    }

    Iterator((Seq.empty, Set(this))) ++ combinations
  }
}
