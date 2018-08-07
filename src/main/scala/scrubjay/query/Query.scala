package scrubjay.query

import scrubjay.datasetid._
import scrubjay.dataspace.DataSpace
import scrubjay.query.constraintsolver.ConstraintSolver._
import scrubjay.query.schema.ScrubJaySchemaQuery

case class Query(dataSpace: DataSpace,
                 queryTarget: ScrubJaySchemaQuery) {

  // TODO: remove repeat solutions

  def solutions: Iterator[DatasetID] = {
    QuerySpace(dataSpace, queryTarget)
      .allSolutions(Query.querySolutions)
      .flatMap(_.solutions)
  }

  def allDerivations: Iterator[DatasetID] = {
    ???
  }
}

object Query {

  // Can I derive a datasource from the set of datasources that satisfies my query?
  lazy val querySolutions: Constraint[DatasetID] = memoize(args => {
    val dataSpace = args(0).as[DataSpace]
    val schemaQuery = args(1).as[ScrubJaySchemaQuery]

    // Run all possible joins of this entire set
    val allJoins = JoinSet.joinedSet(Seq(dataSpace))

    // Run all transformations on allJoins that will yield a valid query solution
    val allJoinsAndTransformations = allJoins.flatMap(dataset => {
      schemaQuery.transformationPaths.map((f: DatasetID => DatasetID) =>
        f(dataset)
      )
    })

    allJoinsAndTransformations.filter(dataset => {
      schemaQuery.matches(dataset.scrubJaySchema)
    })
  })

}
