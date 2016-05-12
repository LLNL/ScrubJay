import scala.util.Random
import scala.collection.immutable.Map

import scrubjay._
import scrubjay.Derivations._

import com.datastax.spark.connector._


object TestScrubJay {
  def main(args: Array[String]) {

    val session = new ScrubJaySession("sonar11")

    val testdata = session.sc.parallelize(Array(
      CassandraRow.fromMap(Map("jobid" -> 123, "starttime" -> "2012-5-14T10:00", "elapsed" -> 20, "nodelist" -> List(1,2,3))),
      CassandraRow.fromMap(Map("jobid" -> 456, "starttime" -> "2012-5-15T10:00", "elapsed" -> 20, "nodelist" -> List(4,5,6)))))

    val testmeta = Array(new Meta("jobid",     "JobID",       "ID"),
                         new Meta("starttime", "StartTime",   "datetime"),
                         new Meta("elapsed",   "ElapsedTime", "seconds"),
                         new Meta("nodelist",  "NodeList",    "ID List"))

    val testds = new DataSource(testdata, testmeta)

    val dds = Derivations.ExpandTimeRange(session.sc, testds)

    println("testds")
    testds.rdd.foreach(println)

    println("dds")
    if (dds.get != None)
      dds.get.rdd.foreach(println)
    else
      println("None")
  }
}
