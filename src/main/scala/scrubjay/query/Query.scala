package scrubjay.query

import com.fasterxml.jackson.databind.ObjectMapper
import scrubjay.datasetid._
import scrubjay.dataspace.DataSpace
import scrubjay.query.constraintsolver.ConstraintSolver._
import scrubjay.query.schema.{ScrubJayColumnSchemaQuery, ScrubJaySchemaQuery}
import com.fasterxml.jackson.module.scala.DefaultScalaModule

case class Query(dataSpace: DataSpace,
                 queryTarget: ScrubJaySchemaQuery) {

  def solutions: Iterator[DatasetID] = {
    QuerySpace(dataSpace, queryTarget)
      .allSolutions(Query.querySolutions)
      .flatMap(_.solutions)
  }

}

object Query {

  def allDerivations(dataSpace: DataSpace): Iterator[DatasetID] = {
    Query(dataSpace, ScrubJaySchemaQuery(Set(ScrubJayColumnSchemaQuery()))).solutions
  }

  // Can I derive a datasource from the set of datasources that satisfies my query?
  lazy val querySolutions: Constraint[DatasetID] = memoize(args => {
    val dataSpace = args(0).as[DataSpace]
    val schemaQuery = args(1).as[ScrubJaySchemaQuery]

    // Run all possible joins of this entire set
    val allJoins = JoinSet.joinedSet(Seq(dataSpace))

    // Run all possible decomposition transformations on each joined dataset
    //   where a decomposition takes a composite dimension like "list<node>"
    //   and transforms it into a simple dimension like "node"
    val allDecompositions = allJoins.flatMap(dataset => {
      dataset.scrubJaySchema.transformationPaths.map((f: DatasetID => DatasetID) =>
        f(dataset)
      )
    })
    // TODO: Too many decompositions here...

    // Run all composition transformations on each above result that will yield a query solution,
    //   where a composition takes one or more simple dimensions like "flopCount, time"
    //   and transforms it into a composite dimension like "rate<flopCount, time>"
    //     (only if "rate<flopCount, time>" is queried)
    val allJoinsAndTransformations = allDecompositions.flatMap(dataset => {
      schemaQuery.transformationPaths.flatMap((f: DatasetID => DatasetID) =>
        if (f(dataset).valid)
          Some(f(dataset))
        else
          None
      )
    })

    // For debugging
    // val mapper = new ObjectMapper()
    // mapper.registerModule(DefaultScalaModule)

    // allJoinsAndTransformations.foreach(d =>
    //   println(
    //     mapper.writeValueAsString(DatasetID.derivationPathJson(d))
    //   )
    // )

    allJoinsAndTransformations.filter(dataset => {
      schemaQuery.matches(dataset.scrubJaySchema)
    })
  })

}
