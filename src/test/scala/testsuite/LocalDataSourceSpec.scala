package testsuite

import scrubjay._
import scrubjay.datasource.LocalDataSource._
import scrubjay.datasource._
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
    lazy val cabLayout = sjs.createLocalDataSource(cabLayoutMeta.keySet.toSeq, cabLayoutRawData, new MetaSource(cabLayoutMeta))

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

    lazy val jobQueueSpan = sjs.deriveTimeSpan(jobQueue)
    lazy val jobQueueSpanExpanded = sjs.deriveExplodedList(jobQueueSpan, List("nodelist"))
    lazy val jobQueueSpanExpandedJoined = sjs.deriveNaturalJoin(jobQueueSpanExpanded, cabLayout)

    describe("Derivations") {

      // Time span

      describe("Job queue with derived time span") {
        it("should be defined") {
          assert(jobQueueSpan.defined)
        }
        it("should match ground truth") {
          assert(jobQueueSpan.rdd.collect.toSet == trueJobQueueSpan)
        }
      }

      // Expanded node list

      describe("Job queue with derived time span AND expanded node list") {
        it("should be defined") {
          assert(jobQueueSpanExpanded.defined)
        }
        it("should match ground truth") {
          assert(jobQueueSpanExpanded.rdd.collect.toSet == trueJobQueueSpanExpanded)
        }
      }

      // Joined with cab layout

      describe("Job queue with derived time span AND expanded node list AND joined with cab layout") {
        it("should be defined") {
          assert(jobQueueSpanExpandedJoined.defined)
        }
        it("should match ground truth") {
          assert(jobQueueSpanExpandedJoined.rdd.collect.toSet == trueJobQueueSpanExpandedJoined)
        }
      }
    }
  }
}
