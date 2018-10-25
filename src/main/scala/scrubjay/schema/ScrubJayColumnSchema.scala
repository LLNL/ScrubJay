// Copyright 2018 Lawrence Livermore National Security, LLC and other
// ScrubJay Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: (Apache-2.0 OR MIT)

package scrubjay.schema

import scrubjay.datasetid.DatasetID

case class ScrubJayColumnSchema(domain: Boolean,
                                name: String = UNKNOWN_STRING,
                                dimension: ScrubJayDimensionSchema = ScrubJayDimensionSchema(),
                                units: ScrubJayUnitsSchema = ScrubJayUnitsSchema()) {

  override def toString: String = {
    s"ScrubJayColumnSchema(domain=$domain, name=$name, dimension=$dimension, units=$units)"
  }

  def generateFieldName: String = {
    val domainType = if (domain) "domain" else "value"
    domainType + ":" + dimension.name + ":" + units.name
  }

  def withGeneratedColumnName: ScrubJayColumnSchema = {
    copy(name = generateFieldName)
  }

  def transformationPaths: Iterator[DatasetID => DatasetID] = {
    units.transformationPaths(name)
  }
}

