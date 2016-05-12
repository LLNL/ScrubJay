import org.apache.spark.SparkContext

import com.datastax.spark.connector._
import com.datastax.spark.connector.rdd._

import com.github.nscala_time.time.Imports._

package scrubjay {

  object derived_datasource {
    def TimeJobNode(sc: SparkContext, ds: DataSource): Option[DataSource] = {

      // Get necessary columns
      val starttime = ds.meta.find(x => (x.globalname == "StartTime" && x.units == "datetime"))
      val endtime   = ds.meta.find(x => (x.globalname == "EndTime"   && x.units == "datetime"))
      val jobid     = ds.meta.find(x => (x.globalname == "JobID"     && x.units == "ID"))
      val nodelist  = ds.meta.find(x => (x.globalname == "NodeList"  && x.units == "ID List"))

      // If columns don't exist, None result
      if (Array(starttime, endtime, jobid, nodelist) contains None) {
        None
      }

      else {

        // Broadcast values
        val starttime_bcast = sc.broadcast(starttime.get.localname)
        val endtime_bcast   = sc.broadcast(endtime.get.localname)
        val jobid_bcast     = sc.broadcast(jobid.get.localname)
        val nodelist_bcast  = sc.broadcast(nodelist.get.localname)

        // Function to create derived rows from a single given row
        def DerivedRows(row: CassandraRow): Seq[CassandraRow] = {
          val starttime_val = DateTime.parse(row.get[String](starttime_bcast.value))
          val endtime_val   = DateTime.parse(row.get[String](endtime_bcast.value))
          val jobid_val     = row.get[Int](jobid_bcast.value)
          val nodelist_val  = row.get[List[Int]](nodelist_bcast.value)

          // All combinations of node x time, for all nodes in nodelist and all times in the time range
          for(node <- nodelist_val; time <- util.DateRange(starttime_val, endtime_val, Period.seconds(1)))
          yield {
              CassandraRow.fromMap(Map("time" -> time,
                                       "jobid" -> jobid_val,
                                       "nodeid" -> node))
          }
        }

        val newmeta = Array(new Meta("time", "Time", "datetime"), 
                            new Meta("jobid", "JobID", "ID"),
                            new Meta("nodeid", "NodeID", "ID"))

        // Derive the new dataset
        Some(new DataSource(ds.rdd.flatMap(DerivedRows), newmeta))
      }
    }
  }
}
