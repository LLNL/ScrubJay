package testsuite

import scrubjay._
import scrubjay.datasource.LocalDataSource._
import scrubjay.derivation.DeriveTimeSpan._
import scrubjay.derivation.ExplodeList._
import scrubjay.derivation.NaturalJoin._
import scrubjay.meta.MetaSource

import org.scalatest._
import org.scalactic.source.Position


class LocalDataSourceSpec extends FunSpec with BeforeAndAfterAll {

  val sjs: ScrubJaySession = new ScrubJaySession()

  override protected def afterAll {
    sjs.sc.stop()
  }

  describe("LocalDataSource") {

    lazy val jobQueue = sjs.createLocalDataSource(jobQueueMeta.keySet.toSeq, jobQueueRawData, new MetaSource(jobQueueMeta))
    lazy val cabLayout = sjs.createLocalDataSource(clusterLayoutMeta.keySet.toSeq, clusterLayoutRawData, new MetaSource(clusterLayoutMeta))

    describe("Creation") {

      // Create local data sources

      describe("Locally generated job queue data") {
        it("should match ground truth") {
          assert(jobQueue.rdd.collect.toSet == trueJobQueue)
        }
      }


      describe("Locally generated cab layout data") {
        it("should match ground truth") {
          assert(cabLayout.rdd.collect.toSet == trueCabLayout)
        }
      }
    }

    describe("Derivations") {

      // Time span
      lazy val jobQueueSpan = deriveTimeSpan(jobQueue, sjs)

      describe("Job queue with derived time span") {
        it("should be defined") {
          assert(jobQueueSpan.isDefined)
        }
        it("should match ground truth") {
          assert(jobQueueSpan.get.rdd.collect.toSet == trueJobQueueSpan)
        }
      }

      // Expanded node list
      lazy val jobQueueSpanExploded = deriveExplodeList(jobQueueSpan.get, List("nodelist"), sjs)

      describe("Job queue with derived time span AND exploded node list") {
        it("should be defined") {
          assert(jobQueueSpanExploded.isDefined)
        }
        it("should match ground truth") {
          assert(jobQueueSpanExploded.get.rdd.collect.toSet == trueJobQueueSpanExploded)
        }
      }

      // Joined with cab layout
      lazy val jobQueueSpanExpandedJoined = deriveNaturalJoin(jobQueueSpanExploded.get, cabLayout, sjs)

      describe("Job queue with derived time span AND exploded node list AND joined with cab layout") {
        it("should be defined") {
          assert(jobQueueSpanExpandedJoined.isDefined)
        }
        it("should match ground truth") {
          assert(jobQueueSpanExpandedJoined.get.rdd.collect.toSet == trueJobQueueSpanExplodedJoined)
        }
      }
    }
  }
}
