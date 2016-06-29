import scala.util.Random
import scala.collection.immutable.Map

import scrubjay._
import scrubjay.datasource._
import scrubjay.cassandra._

import com.datastax.spark.connector._


object TestScrubJay {
  def TestInputLocal(session: ScrubJaySession): DataSource = {

    val testData = session.sc.parallelize(Array(
      Map("jobid"     -> 123, 
          "starttime" -> "2012-5-14T10:00", 
          "elapsed"   -> 20, 
          "nodelist"  -> List(1,2,3)),
      Map("jobid"     -> 456, 
          "starttime" -> "2012-5-15T10:00", 
          "elapsed"   -> 20, 
          "nodelist"  -> List(4,5,6))))

    val testMeta = Map(
      (MetaEntry(session.metaDefinitions.VALUE_JOB_ID, session.metaDefinitions.UNITS_ID)         -> "jobid"),
      (MetaEntry(session.metaDefinitions.VALUE_START_TIME, session.metaDefinitions.UNITS_TIME)   -> "starttime"),
      (MetaEntry(session.metaDefinitions.VALUE_DURATION, session.metaDefinitions.UNITS_SECONDS)  -> "elapsed"),
      (MetaEntry(session.metaDefinitions.VALUE_NODE_LIST, session.metaDefinitions.UNITS_ID_LIST) -> "nodelist"))

    new LocalDataSource(testMeta, testData)
  }

  def TestInputCassandra(session: ScrubJaySession): DataSource = {
    new CassandraDataSource(session.sc, "test", "job_queue")
  }

  def main(args: Array[String]) {

    val session = new ScrubJaySession(
      cassandra_connection = Some(CassandraConnection(hostname = "sonar11")))

    val testds = TestInputLocal(session)
    //val testds = TestInputCassandra(session)

    println("testds")
    //testds.Meta.foreach(println)
    //testds.Data.foreach(println)

    val dds2 = new ExpandedNodeList(testds)

    println("dds2")
    if (dds2.Defined) {
      dds2.Meta.foreach(println)
      dds2.Data.foreach(println)
    }
    else {
      println("UNDEFINED")
    }

    dds2.saveToCassandra(session.sc, "test", "dds2")
  }
}
