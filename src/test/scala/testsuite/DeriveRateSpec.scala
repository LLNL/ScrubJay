// Copyright 2018 Lawrence Livermore National Security, LLC and other
// ScrubJay Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: (Apache-2.0 OR MIT)

package testsuite

import scrubjay.datasetid._
import scrubjay.dataspace.{DataSpace, DimensionSpace}
import scrubjay.datasetid.transformation.DeriveRate
import scrubjay.schema.ScrubJayDimensionSchema


class DeriveRateSpec extends ScrubJaySpec {

  lazy val nodeFlops: DatasetID = DatasetID.fromJsonFile(nodeFlopsDatasetIDFilename)

  lazy val deriveFlops: DatasetID = DeriveRate(nodeFlops, "flops", "time", 2)

  describe("Derive rate") {
    it("should be defined") {
      assert(deriveFlops.valid)
    }
    it("should lookCorrect") {
      println("Before:")
      nodeFlops.debugPrint
      println("After:")
      deriveFlops.debugPrint
    }
    it("should serialize/deserialize correctly") {
      val json: String = DatasetID.toJsonString(deriveFlops)
      println("JSON:")
      println(json)
      assert(DatasetID.fromJsonString(json) == deriveFlops)
    }
  }
}
