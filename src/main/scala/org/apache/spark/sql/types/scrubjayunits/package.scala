// Copyright 2018 Lawrence Livermore National Security, LLC and other
// ScrubJay Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: (Apache-2.0 OR MIT)

package org.apache.spark.sql.types

import org.json4s.JsonAST.JValue

package object scrubjayunits {

  implicit class MetadataExposePrivates(metadata: Metadata) {
    def getMap: Map[String, Any] = metadata.map
    def getElementOrElse[T](key: String, default: T): T = {
      val rawVal = metadata.map.get(key)
      rawVal.fold(default)(_.asInstanceOf[T])
    }
  }

  implicit class DataTypeExposePrivates(dt: DataType) {
    def getJValue: JValue = dt.jsonValue
  }
}
