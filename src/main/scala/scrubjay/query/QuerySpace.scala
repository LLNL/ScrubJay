package scrubjay.query

import gov.llnl.ConstraintSolver.{ArgumentSpace, Arguments}
import scrubjay.datasetid.DatasetID
import scrubjay.dataspace.{DataSpace, DimensionSpace}


case class QuerySpace(dataSpace: DataSpace,
                      domainDimensions: DimensionSpace,
                      valueDimensions: DimensionSpace) extends ArgumentSpace {

  override def enumerate: Iterator[Arguments] = {
    // For all combinations of size 1 to N
    1.to(dataSpace.datasets.length).toIterator.flatMap(
      dataSpace.datasets.combinations(_).map(c => {
        Seq(c.toSet[DatasetID], domainDimensions, valueDimensions)
      })
    )
  }
}
