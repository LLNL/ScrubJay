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


object LocalDataSourceSpec {

  def createLocalJobQueue(sjs: ScrubJaySession): DataSource = {

    val testData = Seq(
      Map(
        "jobid" -> "123",
        "nodelist" -> "1,2,3",
        "elapsed" -> "23",
        "start" -> "2016-08-11T3:30:00+0000",
        "end" -> "2016-08-11T3:30:23+0000"
      ),
      Map(
        "jobid" -> 456,
        "nodelist" -> List(4, 5, 6),
        "elapsed" -> 45,
        "start" -> "2016-08-11T3:30:20+0000",
        "end" -> "2016-08-11T3:31:05+0000"
      ))

    sjs.createLocalDataSource(jobQueueMeta.keySet.toSeq, testData, new MetaSource(jobQueueMeta))
  }

  def createLocalCabLayout(sjs: ScrubJaySession): DataSource = {

    val testData = Seq(
      Map("node" -> 1, "rack" -> 1),
      Map("node" -> 2, "rack" -> 1),
      Map("node" -> 3, "rack" -> 1),
      Map("node" -> 4, "rack" -> 2),
      Map("node" -> 5, "rack" -> 2),
      Map("node" -> 6, "rack" -> 2))

    sjs.createLocalDataSource(cabLayoutMeta.keySet.toSeq, testData, new MetaSource(cabLayoutMeta))
  }
}

class LocalDataSourceSpec extends FunSpec with BeforeAndAfterAll {

  val sjs: ScrubJaySession = new ScrubJaySession()

  override protected def afterAll {
    sjs.sc.stop()
  }

  val jobQueue = LocalDataSourceSpec.createLocalJobQueue(sjs)
  val cabLayout = LocalDataSourceSpec.createLocalCabLayout(sjs)

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

  val jobQueueSpan = sjs.deriveTimeSpan(jobQueue)
  val jobQueueSpanExpanded = sjs.deriveExplodedList(jobQueueSpan, List("nodelist"))
  val jobQueueSpanExpandedJoined = sjs.deriveNaturalJoin(jobQueueSpanExpanded, cabLayout)

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
