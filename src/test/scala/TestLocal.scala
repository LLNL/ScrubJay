import scrubjay._
import scrubjay.datasource.LocalDataSource._
import scrubjay.datasource._
import scrubjay.derivation.ExpandIdentifierList._
import scrubjay.derivation.NaturalJoin._
import scrubjay.meta._

import scala.collection.immutable.Map

// TODO: Proper testing

object TestLocal {

  def createLocalJobQueue(sjs: ScrubJaySession): DataSource = {

    val testData = Array(
      Map(
        "jobid"     -> "123",
        "nodelist"  -> "1,2,3",
        "elapsed"   -> "23",
        "start"     -> "2016-08-11T3:30:00+0000",
        "end"       -> "2016-08-11T3:30:23+0000"
      ),
      Map(
        "jobid"     -> 456,
        "nodelist"  -> List(4,5,6),
        "elapsed"   -> 45,
        "start"     -> "2016-08-11T3:30:20+0000",
        "end"       -> "2016-08-11T3:31:05+0000"
      ))

    val testMeta = Map(
      "jobid" -> MetaEntry.metaEntryFromStringTuple("job", "job", "identifier"),
      "nodelist" -> MetaEntry.metaEntryFromStringTuple("node", "node", "list<identifier>"),
      "elapsed" -> MetaEntry.metaEntryFromStringTuple("duration", "time", "seconds"),
      "start" -> MetaEntry.metaEntryFromStringTuple("start", "time", "datetimestamp"),
      "end" -> MetaEntry.metaEntryFromStringTuple("end", "time", "datetimestamp")
    )

    sjs.createLocalDataSource(testMeta, testData)
  }

  def createLocalCabLayout(sjs: ScrubJaySession): DataSource = {

    val testData = Array(
      Map("node"     -> 1, 
          "rack"     -> 1),
      Map("node"     -> 2, 
          "rack"     -> 1),
      Map("node"     -> 3, 
          "rack"     -> 1),
      Map("node"     -> 4, 
          "rack"     -> 2),
      Map("node"     -> 5, 
          "rack"     -> 2),
      Map("node"     -> 6, 
          "rack"     -> 2))

    val testMeta = Map(
      "node" -> MetaEntry.metaEntryFromStringTuple("node", "node", "identifier"),
      "rack" -> MetaEntry.metaEntryFromStringTuple("rack", "rack", "identifier")
    )

    sjs.createLocalDataSource(testMeta, testData)
  }

  def main(args: Array[String]) {

    // TODO: Query "(SUM duration) PER (rack)"

    val sjs = new ScrubJaySession()

    // Create DataSources
    val jobQueue = createLocalJobQueue(sjs)
    val cabLayout = createLocalCabLayout(sjs)

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

  }
}
