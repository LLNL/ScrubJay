package scrubjay

import org.scalatest.FunSpec
import scrubjay._
import scrubjay.datasource.LocalDataSource._
import scrubjay.datasource._
import scrubjay.derivation.DeriveTimeSpan._
import scrubjay.derivation.ExpandIdentifierList._
import scrubjay.derivation.NaturalJoin._
import scrubjay.meta._

object LocalDataSourceSpec {

  def createLocalJobQueue(sjs: ScrubJaySession): DataSource = {

    val testData = Array(
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

    val testMeta = Map(
      "jobid" -> MetaEntry.fromStringTuple("job", "job", "identifier"),
      "nodelist" -> MetaEntry.fromStringTuple("node", "node", "list<identifier>"),
      "elapsed" -> MetaEntry.fromStringTuple("duration", "time", "seconds"),
      "start" -> MetaEntry.fromStringTuple("start", "time", "datetimestamp"),
      "end" -> MetaEntry.fromStringTuple("end", "time", "datetimestamp")
    )

    sjs.createLocalDataSource(testMeta, testData)
  }

  def createLocalCabLayout(sjs: ScrubJaySession): DataSource = {

    val testData = Array(
      Map("node" -> 1,
        "rack" -> 1),
      Map("node" -> 2,
        "rack" -> 1),
      Map("node" -> 3,
        "rack" -> 1),
      Map("node" -> 4,
        "rack" -> 2),
      Map("node" -> 5,
        "rack" -> 2),
      Map("node" -> 6,
        "rack" -> 2))

    val testMeta = Map(
      "node" -> MetaEntry.fromStringTuple("node", "node", "identifier"),
      "rack" -> MetaEntry.fromStringTuple("rack", "rack", "identifier")
    )

    sjs.createLocalDataSource(testMeta, testData)
  }
}

class LocalDataSourceSpec extends FunSpec {

  //noinspection ConvertNullInitializerToUnderscore
  var sjs: ScrubJaySession = null

  describe("LocalDataSource") {

    sjs = new ScrubJaySession()

    describe("Creation") {

      val jobQueue = LocalDataSourceSpec.createLocalJobQueue(sjs)
      val cabLayout = LocalDataSourceSpec.createLocalCabLayout(sjs)

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

      describe("Derivations") {

        // Time span
        val jobQueueSpan = sjs.deriveTimeSpan(jobQueue)

        describe("Job queue with derived time span") {
          it("should be defined") {
            assert(jobQueueSpan.defined)
          }
          it("should match ground truth") {
            assert(jobQueueSpan.rdd.collect.toSet == trueJobQueueSpan)
          }
        }

        // Expanded node list
        val jobQueueSpanExpanded = sjs.deriveExpandedNodeList(jobQueueSpan, List("nodelist"))

        describe("Job queue with derived time span AND expanded node list") {
          it("should be defined") {
            assert(jobQueueSpanExpanded.defined)
          }
          it("should match ground truth") {
            assert(jobQueueSpanExpanded.rdd.collect.toSet == trueJobQueueSpanExpanded)
          }
        }

        // Joined with cab layout
        val jobQueueSpanExpandedJoined = sjs.deriveNaturalJoin(jobQueueSpanExpanded, cabLayout)

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
}
