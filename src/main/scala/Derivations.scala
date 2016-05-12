import org.apache.spark.SparkContext

import com.datastax.spark.connector._
import com.datastax.spark.connector.rdd._

import com.github.nscala_time.time.Imports._

package scrubjay {

  object Derivations {

    /*
     * Given a datasource with a column describing a list of node IDs,
     *  expand each row into a set of rows, one for each node ID.
     */
    def ExpandNodeList(sc: SparkContext, dso: Option[DataSource]): Option[DataSource] = {

      // Needs Some input
      if (dso == None) {
        None
      }

      else {

        val ds = dso.get

        // Get necessary columns
        val nodelist = ds.meta.find(x => (x.globalname == "NodeList" && x.units == "ID List"))

        // If necessary columns do not exist, None result
        if (Array(nodelist) contains None) {
          None
        }

        else {

          // Broadcast values
          val nodelist_bcast   = sc.broadcast(nodelist.get.localname)

          // Function to create derived rows from a single given row
          def DerivedRows(row: CassandraRow): Seq[CassandraRow] = {

            // Parse values
            val nodelist_val = row.get[List[Int]](nodelist_bcast.value)

            // Create a row for each node in list
            for (node <- nodelist_val) yield {
                CassandraRow.fromMap(row.toMap + ("node" -> node))
              }
          }

          // Resulting metadata
          val resultmeta = ds.meta :+ new Meta("node", "NodeID", "ID")
          
          // Create the derived dataset
          Some(new DataSource(ds.rdd.flatMap(DerivedRows), resultmeta))
        }
      }
    }

    /*
     * Given a datasource with a start time and number of elapsed seconds,
     *  expand each row into a set of rows, one for each instantaneous second in the 
     *  elapsed time range.
     */
    def ExpandTimeRange(sc: SparkContext, dso: Option[DataSource]): Option[DataSource] = {

      // Needs Some input
      if (dso == None) {
        None
      }

      else {

        val ds = dso.get

        // Get necessary columns
        val starttime     = ds.meta.find(x => (x.globalname == "StartTime"   && x.units == "datetime"))
        val elapsedtime   = ds.meta.find(x => (x.globalname == "ElapsedTime" && x.units == "seconds"))

        // If necessary columns do not exist, None result
        if (Array(starttime, elapsedtime) contains None) {
          None
        }

        else {

          // Broadcast values
          val starttime_bcast   = sc.broadcast(starttime.get.localname)
          val elapsedtime_bcast = sc.broadcast(elapsedtime.get.localname)

          // Function to create derived rows from a single given row
          def DerivedRows(row: CassandraRow): Seq[CassandraRow] = {

            // Parse values
            val starttime_val   = DateTime.parse(row.get[String](starttime_bcast.value))
            val elapsedtime_val = row.get[Int](elapsedtime_bcast.value)

            // Create a timestamp for each second in the time range
            val timerange = util.DateRange(starttime_val,               
                                           starttime_val + elapsedtime_val.seconds, 
                                           Period.seconds(1))

            // Iterate over each timestamp and create a new row for each
            for (time <- timerange.toList) yield {
                CassandraRow.fromMap(row.toMap + ("time" -> time))
              }
          }

          // Resulting metadata
          val resultmeta = ds.meta :+ new Meta("time", "Time", "datetime")
          
          // Create the derived dataset
          Some(new DataSource(ds.rdd.flatMap(DerivedRows), resultmeta))
        }
      }
    }
  }
}
