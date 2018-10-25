// Copyright 2018 Lawrence Livermore National Security, LLC and other
// ScrubJay Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: (Apache-2.0 OR MIT)

package scrubjay.query.schema

import scrubjay.datasetid.DatasetID
import scrubjay.datasetid.transformation.{DeriveRate, ExplodeList, ExplodeRange}
import scrubjay.query.constraintsolver.Combinatorics
import scrubjay.schema.ScrubJayDimensionSchema

case class ScrubJayDimensionSchemaQuery(name: Option[String] = None,
                                        ordered: Option[Boolean] = None,
                                        continuous: Option[Boolean] = None,
                                        subDimensions: Option[Seq[ScrubJayDimensionSchemaQuery]] = None) {

  def matches(scrubJayDimensionSchema: ScrubJayDimensionSchema): Boolean = {
    val nameMatches = wildMatch(scrubJayDimensionSchema.name, name)
    val orderedMatches = wildMatch(scrubJayDimensionSchema.ordered, ordered)
    val continuousMatches = wildMatch(scrubJayDimensionSchema.continuous, continuous)
    val subDimensionsMatch = subDimensions.isEmpty ||
      subDimensions.get.forall(q => scrubJayDimensionSchema.subDimensions.exists(d => q.matches(d)))
    nameMatches && orderedMatches && continuousMatches && subDimensionsMatch
  }

  lazy val singleTransformation: DatasetID => DatasetID = {
    name.get match {
      case "rate" =>
        (dsID: DatasetID) =>
          DeriveRate(dsID, subDimensions.get(0).name.get, subDimensions.get(1).name.get)
    }
  }

  def transformationPaths: Iterator[DatasetID => DatasetID] = {
    Combinatorics.decompositions(
      this,
      (q: ScrubJayDimensionSchemaQuery) => {
        q.name.isDefined && q.subDimensions.isDefined && q.subDimensions.get.nonEmpty
      },
      (q: ScrubJayDimensionSchemaQuery) => {
        q.subDimensions.get
      },
      (q: ScrubJayDimensionSchemaQuery) => {
        q.singleTransformation
      }
    )
  }

}
