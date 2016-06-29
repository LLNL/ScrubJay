import scala.util.Random
import scala.collection.immutable.Map

import scrubjay._
import scrubjay.localDataSource._
import scrubjay.expandedNodeList._
import scrubjay.datasource._
import scrubjay.cassandraDataSource._

import com.datastax.spark.connector._


object TestScrubJay {
  def TestInputLocal(sjs: ScrubJaySession): DataSource = {

    val testData = Array(
      Map("jobid"     -> 123, 
          "starttime" -> "2012-5-14T10:00", 
          "elapsed"   -> 20, 
          "nodelist"  -> List(1,2,3)),
      Map("jobid"     -> 456, 
          "starttime" -> "2012-5-15T10:00", 
          "elapsed"   -> 20, 
          "nodelist"  -> List(4,5,6)))

    val testMeta = Map(
      (MetaEntry(sjs.metaOntology.VALUE_JOB_ID, sjs.metaOntology.UNITS_ID)         -> "jobid"),
      (MetaEntry(sjs.metaOntology.VALUE_START_TIME, sjs.metaOntology.UNITS_TIME)   -> "starttime"),
      (MetaEntry(sjs.metaOntology.VALUE_DURATION, sjs.metaOntology.UNITS_SECONDS)  -> "elapsed"),
      (MetaEntry(sjs.metaOntology.VALUE_NODE_LIST, sjs.metaOntology.UNITS_ID_LIST) -> "nodelist"))

    sjs.createLocalDataSource(testMeta, testData)
  }

  def TestInputCassandra(sjs: ScrubJaySession): DataSource = {
    sjs.createCassandraDataSource(Map[MetaEntry,String](), "test", "job_queue")
  }

  def main(args: Array[String]) {

    val sjs = new ScrubJaySession(
      cassandra_connection = Some(CassandraConnection(hostname = "sonar11")))

    val testds = TestInputLocal(sjs)
    //val testds = TestInputCassandra(sjs)

    println("testds")
    testds.metaMap.foreach(println)
    testds.rdd.foreach(println)

    val dds2 = sjs.deriveExpandedNodeList(testds)

    println("dds2")
    if (dds2.defined) {
      dds2.metaMap.foreach(println)
      dds2.rdd.foreach(println)
    }
    else {
      println("UNDEFINED")
    }

    dds2.saveToCassandra(sjs.sc, "test", "dds2")
  }
}
