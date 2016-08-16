import scrubjay._
import scrubjay.datasource.CSVDataSource._
import scrubjay.datasource._
import scrubjay.derivation.ExpandIdentifierList._
import scrubjay.derivation.NaturalJoin._
import scrubjay.meta._

import java.io._

object TestCSV {

  def createCSVJobQueue(sjs: ScrubJaySession): DataSource = {

    val testDataFileName = "jobqueue.csv"
    val fileWriter = new PrintWriter(new FileOutputStream(testDataFileName, false))

    fileWriter.println("jobid, nodelist, elapsed, start, end")
    fileWriter.println("123, \"1,2,3\", 23, 2016-08-11T3:30:00+0000, 2016-08-11T3:30:23+0000")
    fileWriter.println("456, \"4,5,6\", 45, 2016-08-11T3:30:20+0000, 2016-08-11T3:31:05+0000")

    fileWriter.close()

    val testMeta = Map(
      "jobid" -> MetaEntry.fromStringTuple("job", "job", "identifier"),
      "nodelist" -> MetaEntry.fromStringTuple("node", "node", "list<identifier>"),
      "elapsed" -> MetaEntry.fromStringTuple("duration", "time", "seconds"),
      "start" -> MetaEntry.fromStringTuple("start", "time", "datetimestamp"),
      "end" -> MetaEntry.fromStringTuple("end", "time", "datetimestamp")
    )

    sjs.createCSVDataSource(testMeta, testDataFileName)
  }

  def createCSVCabLayout(sjs: ScrubJaySession): DataSource = {

    val testDataFileName = "cablayout.csv"
    val fileWriter = new PrintWriter(new FileOutputStream(testDataFileName, false))

    fileWriter.println("node, rack")
    fileWriter.println("1,1")
    fileWriter.println("2,1")
    fileWriter.println("3,1")
    fileWriter.println("4,2")
    fileWriter.println("5,2")
    fileWriter.println("6,2")

    fileWriter.close()

    val testMeta = Map(
      "node" -> MetaEntry.fromStringTuple("node", "node", "identifier"),
      "rack" -> MetaEntry.fromStringTuple("rack", "rack", "identifier")
    )

    sjs.createCSVDataSource(testMeta, testDataFileName)
  }

  def main(args: Array[String]) {

    // TODO: Query "(SUM duration) PER (rack)"

    val sjs = new ScrubJaySession()

    // Create DataSources
    val jobQueue = createCSVJobQueue(sjs)
    val cabLayout = createCSVCabLayout(sjs)

    println("********* jobQueue *********")
    jobQueue.rdd.foreach(println)

    println("********* cabLayout *********")
    cabLayout.rdd.foreach(println)

    val jobQueueExpanded = sjs.deriveExpandedNodeList(jobQueue, List("nodelist"))

    println("********* jobQueueExpanded *********")
    if (jobQueueExpanded.defined)
      jobQueueExpanded.rdd.foreach(println)
    else
      println("undefined!")

    val jobQueueJoined = sjs.deriveNaturalJoin(jobQueueExpanded, cabLayout)

    println("********* jobQueueExpandedJoined *********")
    if (jobQueueJoined.defined)
      jobQueueJoined.rdd.foreach(println)
    else
      println("undefined!")

    sjs.sc.stop()
  }
}

