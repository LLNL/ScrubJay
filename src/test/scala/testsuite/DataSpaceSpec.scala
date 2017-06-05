package testsuite

import scrubjay.datasetid.combination._
import scrubjay.datasetid.transformation._
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

    it("should create the correct dataframes") {
      jobData.datasets.foreach(_.realize(jobData.dimensionSpace).show(false))
    }

    it("should correctly execute a derivation path") {
      val exploded = ExplodeContinuousRange(ExplodeDiscreteRange(jobData.datasets.head, "domain:node:list"), "domain:time:range", 30000)
      //exploded.debugPrint(jobData.dimensionSpace)

      val naturaljoined = NaturalJoin(exploded, jobData.datasets(1))
      //naturaljoined.debugPrint(jobData.dimensionSpace)

      val interjoined1 = InterpolationJoin(naturaljoined, jobData.datasets(2), 60)
      //interjoined1.debugPrint(jobData.dimensionSpace)

      jobData.datasets(2).debugPrint(jobData.dimensionSpace)
      naturaljoined.debugPrint(jobData.dimensionSpace)
      val interjoined2 = InterpolationJoin(jobData.datasets(2), naturaljoined, 60)
      interjoined2.debugPrint(jobData.dimensionSpace)
    }
  }
}
