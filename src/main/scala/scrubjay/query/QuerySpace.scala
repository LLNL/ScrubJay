package scrubjay.query

import gov.llnl.ConstraintSolver.{ArgumentSpace, Arguments}
import scrubjay.datasource.DataSourceID
import scrubjay.metabase.MetaEntry
import scrubjay.transformation.UberExplode


case class QuerySpace(metaEntries: Set[MetaEntry], dsIDs: Seq[DataSourceID]) extends ArgumentSpace {

  override def enumerate: Iterator[Arguments] = {

    val explodedIDs: Seq[DataSourceID] = dsIDs.flatMap(dsID => UberExplode(dsID))
    val originalAndDerivedIDs: Seq[DataSourceID] = dsIDs ++ explodedIDs

    // From 1 to N datasources at a time
    1.to(originalAndDerivedIDs.length).toIterator.flatMap(
      // For all combinations of size N
      originalAndDerivedIDs.combinations(_).map(c => Seq(metaEntries, c.toSet[DataSourceID]))
    )
  }

}

case class DataSourceArgumentSpace(dsIDs: Seq[DataSourceID]) extends ArgumentSpace {

  override def enumerate: Iterator[Arguments] = {
    QuerySpace(Set.empty, dsIDs).enumerate.map(args => Seq(args(1)))
  }

}

