// Copyright 2018 Lawrence Livermore National Security, LLC and other
// ScrubJay Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: (Apache-2.0 OR MIT)

package testsuite

import scrubjay.dataspace.DataSpace

class DataSpaceSpec extends ScrubJaySpec {

  lazy val jobData: DataSpace = DataSpace.fromJsonFile(jobAnalysisDataSpaceFilename)

  describe("Load ScrubJay DatasetID from .sj file") {
    it("should serialize/deserialize correctly") {
      val json = jobData.toJsonString
      println(json)
      val reloaded = DataSpace.fromJsonString(json)
      // TODO: implement isEqual for DataSpace
      // assert(reloaded == jobData)
    }

    it("should create the correct datasets") {
      jobData.datasets.foreach(_.debugPrint)
    }
  }
}
