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
      .allSolutions(Query.joinableDsIDSet)
      .flatMap(_.solutions)
  }

  def allDerivations: Iterator[DatasetID] = {
    ???
  }
}

object Query {

  lazy val satisfiesQuery: Constraint[DatasetID] = memoize(args => {
    val dataSpace = args(0).as[DataSpace]
    val queryTarget = args(1).as[ScrubJaySchemaQuery]

    // Case 1: no new columns need to be derived, just need to integrate existing ones

    // Case 2: new columns need to be derived, determine their inputDatasetIDs and first integrate them, then derive

    // Determine what columns need to be integrated before deriving new ones

    ???

  })

  // Can I derive a datasource from the set of datasources that satisfies my query?
  lazy val joinableDsIDSet: Constraint[DatasetID] = memoize(args => {
    val dataSpace = args(0).as[DataSpace]
    val queryTarget = args(1).as[ScrubJaySchemaQuery]

    // Run all possible joins of this entire set
    val allJoins = JoinSet.joinedSet(Seq(dataSpace))

    // Run all derivations on the joined results
    // val allDerivations = allJoins.flatMap(dataset => {
    //   dataset.scrubJaySchema(dataSpace.dimensions).columnNames.flatMap(column => {
    //     val explodeDiscreteRange = ExplodeList(dataset, column)
    //     if (explodeDiscreteRange.isValid(dataSpace.dimensions)) {
    //       Some(explodeDiscreteRange)
    //     } else {
    //       None
    //     }
    //   })
    // })

    // Return all joins and derivations that satisfy the query
    allJoins.filter(dataset => {
      dataset.scrubJaySchema.matchesQuery(queryTarget)
    })
  })

}
