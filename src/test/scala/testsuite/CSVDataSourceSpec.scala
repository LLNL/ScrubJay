package testsuite

import scrubjay._
import scrubjay.datasource.CSVDataSource._
import scrubjay.derivation.DeriveTimeSpan._
import scrubjay.derivation.ExplodeList._
import scrubjay.derivation.NaturalJoin._

import scrubjay.metasource.CSVMetaSource._

import org.scalatest._
import org.scalactic.source.Position

import java.io._


class CSVDataSourceSpec extends FunSpec with BeforeAndAfterAll {

  val sjs: ScrubJaySession = new ScrubJaySession()

  val jobQueueMetaFile = new File("jobqueuemeta.csv")
  val clusterLayoutMetaFile = new File("clusterlayoutmeta.csv")
  val jobQueueDataFile = new File("jobqueue.csv")
  val clusterLayoutDataFile = new File("clusterlayout.csv")

  override protected def beforeAll(): Unit = {
    {
      val fileWriter = new PrintWriter(jobQueueMetaFile)
      fileWriter.println("column, meaning, dimension, units")
      fileWriter.println("jobid, job, job, identifier")
      fileWriter.println("nodelist, node, node, list<identifier>")
      fileWriter.println("elapsed, duration, time, seconds")
      fileWriter.println("start, start, time, datetimestamp")
      fileWriter.println("end, end, time, datetimestamp")
      fileWriter.close()
    }

    {
      val fileWriter = new PrintWriter(clusterLayoutMetaFile)
      fileWriter.println("column, meaning, dimension, units")
      fileWriter.println("node, node, node, identifier")
      fileWriter.println("rack, rack, rack, identifier")
      fileWriter.close()
    }

    {
      val fileWriter = new PrintWriter(jobQueueDataFile)
      fileWriter.println("jobid, nodelist, elapsed, start, end")
      fileWriter.println("123, \"1,2,3\", 23, 2016-08-11T3:30:00+0000, 2016-08-11T3:31:00+0000")
      fileWriter.println("456, \"4,5,6\", 45, 2016-08-11T3:30:00+0000, 2016-08-11T3:32:00+0000")
      fileWriter.close()
    }

    {
      val fileWriter = new PrintWriter(clusterLayoutDataFile)
      fileWriter.println("node, rack")
      fileWriter.println("1,1")
      fileWriter.println("2,1")
      fileWriter.println("3,1")
      fileWriter.println("4,2")
      fileWriter.println("5,2")
      fileWriter.println("6,2")
      fileWriter.close()
    }
  }

  override protected def afterAll {
    sjs.sc.stop()
    jobQueueDataFile.delete()
    clusterLayoutDataFile.delete()
    jobQueueMetaFile.delete()
    clusterLayoutMetaFile.delete()
  }

  describe("CSVDataSource") {

    lazy val jobQueueMetaSource = createCSVMetaSource(jobQueueMetaFile.getName)
    lazy val clusterLayoutMetaSource = createCSVMetaSource(clusterLayoutMetaFile.getName)
    lazy val jobQueue = sjs.createCSVDataSource(jobQueueDataFile.getName, jobQueueMetaSource)
    lazy val cabLayout = sjs.createCSVDataSource(clusterLayoutDataFile.getName, clusterLayoutMetaSource)

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

      // Exploded node list
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
      lazy val jobQueueSpanExplodedJoined = deriveNaturalJoin(jobQueueSpanExploded.get, cabLayout, sjs)

      describe("Job queue with derived time span AND exploded node list AND joined with cab layout") {
        it("should be defined") {
          assert(jobQueueSpanExplodedJoined.isDefined)
        }
        it("should match ground truth") {
          assert(jobQueueSpanExplodedJoined.get.rdd.collect.toSet == trueJobQueueSpanExplodedJoined)
        }
      }
    }
  }
}
