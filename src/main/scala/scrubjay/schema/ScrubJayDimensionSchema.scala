// Copyright 2018 Lawrence Livermore National Security, LLC and other
// ScrubJay Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: (Apache-2.0 OR MIT)

package scrubjay.schema

case class ScrubJayDimensionSchema(name: String = UNKNOWN_STRING,
                                   ordered: Boolean = false,
                                   continuous: Boolean = false,
                                   subDimensions: Seq[ScrubJayDimensionSchema] = Seq.empty) {
}
