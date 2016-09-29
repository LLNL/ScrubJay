package testsuite

import scrubjay._
import scrubjay.datasource.CSVDataSource._
import scrubjay.datasource._
import scrubjay.derivation.DeriveTimeSpan._
import scrubjay.derivation.ExplodeList._
import scrubjay.derivation.NaturalJoin._

import scrubjay.meta._
import scrubjay.meta.CSVMetaSource._

import org.scalatest._
import org.scalactic.source.Position

import java.io._


object CSVDataSourceSpec {

  def createJobQueueCSVMetaSource: MetaSource = {

    val fileName = "jobqueuemeta.csv"
    val file = new File(fileName)

    try {
      val fileWriter = new PrintWriter(file)

      fileWriter.println("column, meaning, dimension, units")
      fileWriter.println("jobid, job, job, identifier")
      fileWriter.println("nodelist, node, node, list<identifier>")
      fileWriter.println("elapsed, duration, time, seconds")
      fileWriter.println("start, start, time, datetimestamp")
      fileWriter.println("end, end, time, datetimestamp")
      fileWriter.close()

      createCSVMetaSource(fileName)
    }
    finally {
      file.delete()
    }
  }

  def createCabLayoutCSVMetaSource: MetaSource = {

    val fileName = "cablayoutmeta.csv"
    val file = new File(fileName)

    try {
      val fileWriter = new PrintWriter(file)

      fileWriter.println("column, meaning, dimension, units")
      fileWriter.println("node, node, node, identifier")
      fileWriter.println("rack, rack, rack, identifier")
      fileWriter.close()

      createCSVMetaSource(fileName)
    }
    finally {
      file.delete()
    }
  }

  def createJobQueueCSVDataSource(sjs: ScrubJaySession): DataSource = {

    val fileName = "jobqueue.csv"
    val file = new File(fileName)

    try {
      val fileWriter = new PrintWriter(file)

      fileWriter.println("jobid, nodelist, elapsed, start, end")
      fileWriter.println("123, \"1,2,3\", 23, 2016-08-11T3:30:00+0000, 2016-08-11T3:30:23+0000")
      fileWriter.println("456, \"4,5,6\", 45, 2016-08-11T3:30:20+0000, 2016-08-11T3:31:05+0000")
      fileWriter.close()

      sjs.createCSVDataSource(fileName, createJobQueueCSVMetaSource)
    }
    finally {
      file.delete()
    }
  }

  def createCabLayoutCSVDataSource(sjs: ScrubJaySession): DataSource = {

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

      sjs.createCSVDataSource(fileName, createCabLayoutCSVMetaSource)
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

    lazy val jobQueue = CSVDataSourceSpec.createJobQueueCSVDataSource(sjs)
    lazy val cabLayout = CSVDataSourceSpec.createCabLayoutCSVDataSource(sjs)

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
