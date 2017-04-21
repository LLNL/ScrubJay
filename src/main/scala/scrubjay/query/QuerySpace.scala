package scrubjay.query

import gov.llnl.ConstraintSolver.{ArgumentSpace, Arguments}
import scrubjay.datasource.DatasetID
//import scrubjay.transformation.UberExplode


case class QuerySpace(metaEntries: Set[(String, String)], dsIDs: Seq[DatasetID]) extends ArgumentSpace {

  override def enumerate: Iterator[Arguments] = {

    /*
    val explodedIDs: Seq[DatasetID] = dsIDs.flatMap(dsID => UberExplode(dsID))
    val originalAndDerivedIDs: Seq[DatasetID] = dsIDs ++ explodedIDs

    // From 1 to N datasources at a time
    1.to(originalAndDerivedIDs.length).toIterator.flatMap(
      // For all combinations of size N
      originalAndDerivedIDs.combinations(_).map(c => Seq(metaEntries, c.toSet[DatasetID]))
    )
    */
    ???
  }

}

case class DataSourceArgumentSpace(dsIDs: Seq[DatasetID]) extends ArgumentSpace {

  override def enumerate: Iterator[Arguments] = {
    QuerySpace(Set.empty, dsIDs).enumerate.map(args => Seq(args(1)))
  }

}

