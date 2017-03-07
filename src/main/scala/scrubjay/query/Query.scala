package scrubjay.query

import scrubjay.metabase._
import scrubjay.datasource._

import gov.llnl.ConstraintSolver._


case class Query(dataSources: Set[DataSourceID],
                 metaEntries: Set[MetaEntry]) {

  // Can I derive a datasource from the set of datasources that satisfies my query?
  lazy val dsIDSetSatisfiesQuery: Constraint[DataSourceID] = memoize(args => {
    val query = args(0).as[Set[MetaEntry]]
    val dsIDSet = args(1).as[Set[DataSourceID]]

    // Fun case: queried meta entries exist in a data source derived from multiple data sources
    val dsIDMeta = dsIDSet.toSeq.map(_.metaSource.values.toSet).reduce(_ union _)
    val metaSatisfied = query.intersect(dsIDMeta).size == query.size

    if (metaSatisfied)
      JoinSpace.joinedSet(Seq(dsIDSet))
    else
      Seq.empty
  })

  def solutions: Iterator[DataSourceID] = {
    QuerySpace(metaEntries, dataSources.toSeq)
      .allSolutions(dsIDSetSatisfiesQuery).flatMap(_.solutions)
  }

  def allDerivations: Iterator[DataSourceID] = {
    DataSourceArgumentSpace(dataSources.toSeq)
      .allSolutions(JoinSpace.joinedSet).flatMap(_.solutions)
  }
}
