package testsuite

import scrubjay._
import scrubjay.datasource.CSVDataSource._
import scrubjay.datasource._
import scrubjay.derivation.DeriveTimeSpan._
import scrubjay.derivation.ExplodeList._
import scrubjay.derivation.NaturalJoin._
import scrubjay.meta.MetaSource

import org.scalatest._
import org.scalactic.source.Position

import java.io._


object CSVDataSourceSpec {

  def createCSVJobQueue(sjs: ScrubJaySession): DataSource = {

    val fileName = "jobqueue.csv"
    val file = new File(fileName)

    try {
      val fileWriter = new PrintWriter(file)

      fileWriter.println("jobid, nodelist, elapsed, start, end")
      fileWriter.println("123, \"1,2,3\", 23, 2016-08-11T3:30:00+0000, 2016-08-11T3:30:23+0000")
      fileWriter.println("456, \"4,5,6\", 45, 2016-08-11T3:30:20+0000, 2016-08-11T3:31:05+0000")
      fileWriter.close()

      sjs.createCSVDataSource(fileName, new MetaSource(jobQueueMeta))
    }
    finally {
      file.delete()
    }
  }

  def createCSVCabLayout(sjs: ScrubJaySession): DataSource = {

    val fileName = "cablayout.csv"
    val file = new File(fileName)

    try {
      val fileWriter = new PrintWriter(file)
      fileWriter.println("node, rack")
      fileWriter.println("1,1")
      fileWriter.println("2,1")
      fileWriter.println("3,1")
      fileWriter.println("4,2")
      fileWriter.println("5,2")
      fileWriter.println("6,2")
      fileWriter.close()

      sjs.createCSVDataSource(fileName, new MetaSource(cabLayoutMeta))
    }
    finally {
      file.delete()
    }
  }
}

class CSVDataSourceSpec extends FunSpec with BeforeAndAfterAll {

  val sjs: ScrubJaySession = new ScrubJaySession()

  override protected def afterAll {
    sjs.sc.stop()
  }

  describe("CSVDataSource") {

    val jobQueue = CSVDataSourceSpec.createCSVJobQueue(sjs)
    val cabLayout = CSVDataSourceSpec.createCSVCabLayout(sjs)

    describe("Creation") {

      // Create DataSources

      describe("CSV sourced job queue data") {
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
}
