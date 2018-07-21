package scrubjay.query

import gov.llnl.ConstraintSolver.{ArgumentSpace, Arguments}
import scrubjay.dataspace.{DataSpace, DimensionSpace}
import scrubjay.query.schema.ScrubJaySchemaQuery


case class QuerySpace(dataSpace: DataSpace,
                      target: ScrubJaySchemaQuery) extends ArgumentSpace {

  override def enumerate: Iterator[Arguments] = {
    // For all combinations of size 1 to N
    1.to(dataSpace.datasets.length).toIterator.flatMap(
      dataSpace.datasets.combinations(_).map(c => {
        Seq(DataSpace(dataSpace.dimensionSpace, c), target)
      })
    )
  }
}
