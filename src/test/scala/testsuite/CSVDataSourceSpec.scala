package testsuite

import scrubjay._
import org.scalactic.source.Position
import java.io._

import scrubjay.datasource.CSVDataSource

class CSVDataSourceSpec extends ScrubJaySpec {

  val jobQueueMetaFile = new File("jobqueuemeta.csv")
  val clusterLayoutMetaFile = new File("clusterlayoutmeta.csv")

  val jobQueueDataFile = new File("jobqueue.csv")
  val clusterLayoutDataFile = new File("clusterlayout.csv")

  override protected def beforeAll: Unit = {
    super.beforeAll()

    {
      val fileWriter = new PrintWriter(jobQueueMetaFile)
      fileWriter.println("column, relationType, meaning, dimension, units")
      fileWriter.println("jobid, domain, job, job, identifier")
      fileWriter.println("nodelist, domain, node, node, list<identifier>")
      fileWriter.println("elapsed, value, duration, time, seconds")
      fileWriter.println("start, domain, start, time, datetimestamp")
      fileWriter.println("end, domain, end, time, datetimestamp")
      fileWriter.close()
    }

    {
      val fileWriter = new PrintWriter(clusterLayoutMetaFile)
      fileWriter.println("column, relationType, meaning, dimension, units")
      fileWriter.println("node, domain, node, node, identifier")
      fileWriter.println("rack, domain, rack, rack, identifier")
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
    super.afterAll()
    
    jobQueueDataFile.delete()
    clusterLayoutDataFile.delete()
    jobQueueMetaFile.delete()
    clusterLayoutMetaFile.delete()
  }

  lazy val jobQueueMetaSource: MetaSource = sc.createCSVMetaSource(jobQueueMetaFile.getName)
  lazy val clusterLayoutMetaSource: MetaSource = sc.createCSVMetaSource(clusterLayoutMetaFile.getName)

  lazy val jobQueue: DataSourceID = sc.createCSVDataSource(jobQueueDataFile.getName, jobQueueMetaSource)
  lazy val cabLayout: DataSourceID = sc.createCSVDataSource(clusterLayoutDataFile.getName, clusterLayoutMetaSource)

  describe("CSV sourced job queue data") {
    it("should match ground truth") {
      assert(jobQueue.realize.collect.toSet == trueJobQueue)
    }
  }

  describe("Locally generated cab layout data") {
    it("should match ground truth") {
      assert(cabLayout.realize.collect.toSet == trueCabLayout)
    }
  }
}
