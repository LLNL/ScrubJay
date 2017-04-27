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
  }
}
