package scrubjay.query.schema

import scrubjay.query.constraintsolver.Combinatorics

case class ScrubJaySchemaQuery (columns: Set[ScrubJayColumnSchemaQuery]) {
  // def expand: Iterator[ScrubJaySchemaQuery] = {
  //   val columnExpansions: Seq[Seq[Set[ScrubJayColumnSchemaQuery]]] = columns.map(column => column.expand.toSeq).toSeq
  //   Combinatorics.cartesian(columnExpansions)
  //     .map((c: Seq[Set[ScrubJayColumnSchemaQuery]]) => c.reduce((a, b) => a union b))
  //     .map((c: Set[ScrubJayColumnSchemaQuery]) => ScrubJaySchemaQuery(c))
  // }
}
