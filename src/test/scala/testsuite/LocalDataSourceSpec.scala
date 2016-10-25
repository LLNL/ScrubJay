package testsuite

import scrubjay._

import org.scalactic.source.Position

class LocalDataSourceSpec extends ScrubJaySpec {

  describe("LocalDataSource") {

    lazy val jobQueue = sc.createLocalDataSource(jobQueueRawData, jobQueueMeta.keySet.toSeq, new MetaSource(jobQueueMeta))
    lazy val cabLayout = sc.createLocalDataSource(clusterLayoutRawData, clusterLayoutMeta.keySet.toSeq, new MetaSource(clusterLayoutMeta))
    lazy val nodeFlops = sc.createLocalDataSource(nodeDataRawData, nodeDataMeta.keySet.toSeq, new MetaSource(nodeDataMeta))

    describe("Creation") {

      // Create local data sources

      describe("Locally generated job queue data") {
        it("should be defined") {
          assert(jobQueue.isDefined)
        }
        it("should match ground truth") {
          assert(jobQueue.get.rdd.collect.toSet == trueJobQueue)
        }
      }


      describe("Locally generated cab layout data") {
        it("should be defined") {
          assert(cabLayout.isDefined)
        }
        it("should match ground truth") {
          assert(cabLayout.get.rdd.collect.toSet == trueCabLayout)
        }
      }
    }

    describe("Derivations") {

      // Time span
      lazy val jobQueueSpan = jobQueue.get.deriveTimeSpan

      describe("Job queue with derived time span") {
        it("should be defined") {
          assert(jobQueueSpan.isDefined)
        }
        it("should match ground truth") {
          assert(jobQueueSpan.get.rdd.collect.toSet == trueJobQueueSpan)
        }
      }

      // Expanded node list
      lazy val jobQueueSpanExploded = jobQueueSpan.get.deriveExplodeList(Seq("nodelist"))

      describe("Job queue with derived time span AND exploded node list") {
        it("should be defined") {
          assert(jobQueueSpanExploded.isDefined)
        }
        it("should match ground truth") {
          assert(jobQueueSpanExploded.get.rdd.collect.toSet == trueJobQueueSpanExploded)
        }
      }

      // Joined with cab layout
      lazy val jobQueueSpanExplodedJoined = jobQueueSpanExploded.get.deriveNaturalJoin(cabLayout)

      describe("Job queue with derived time span AND exploded node list AND joined with cab layout") {
        it("should be defined") {
          assert(jobQueueSpanExplodedJoined.isDefined)
        }
        it("should match ground truth") {
          assert(jobQueueSpanExplodedJoined.get.rdd.collect.toSet == trueJobQueueSpanExplodedJoined)

          println
          jobQueueSpanExplodedJoined.get.metaSource.metaEntryMap.foreach(kv => println(kv._1 + " -> " + kv._2.units + ", " + kv._2.dimension + ", " + kv._2.units.unitsTag.domainType))
          println
          nodeFlops.get.metaSource.metaEntryMap.foreach(kv => println(kv._1 + " -> " + kv._2.units + ", " + kv._2.dimension + ", " + kv._2.units.unitsTag.domainType))
        }
      }


      lazy val jobQueueSpanExplodedJoinedFlops = jobQueueSpanExplodedJoined.get.deriveRangeJoin(nodeFlops)

      describe("Job queue with derived time span AND exploded node list AND joined with cab layout AND range-joined with node FLOPs") {
        it("should be defined") {
          assert(jobQueueSpanExplodedJoinedFlops.isDefined)

          jobQueueSpanExplodedJoinedFlops.get.rdd.collect.foreach(println)
        }
      }
    }
  }
}
