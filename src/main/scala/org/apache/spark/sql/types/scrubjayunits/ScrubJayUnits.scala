// Copyright 2018 Lawrence Livermore National Security, LLC and other
// ScrubJay Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: (Apache-2.0 OR MIT)

package org.apache.spark.sql.types.scrubjayunits

import org.apache.spark.sql.types.DataType

trait ScrubJayUDTObject {
  def sqlType: DataType
}

abstract class ScrubJayUnits extends Serializable {
}
