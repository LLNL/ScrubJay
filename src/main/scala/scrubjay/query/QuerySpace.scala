// Copyright 2018 Lawrence Livermore National Security, LLC and other
// ScrubJay Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: (Apache-2.0 OR MIT)

package scrubjay.query

import scrubjay.query.constraintsolver.ConstraintSolver._
import scrubjay.dataspace.{DataSpace, DimensionSpace}
import scrubjay.query.schema.ScrubJaySchemaQuery


case class QuerySpace(dataSpace: DataSpace,
                      target: ScrubJaySchemaQuery) extends ArgumentSpace {

  override def enumerate: Iterator[Arguments] = {
    // For all combinations of size 1 to N
    1.to(dataSpace.datasets.length).toIterator.flatMap(
      dataSpace.datasets.combinations(_).map(c => {
        Seq(DataSpace(c), target)
      })
    )
  }
}
