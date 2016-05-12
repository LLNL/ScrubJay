import org.apache.spark.SparkContext

import com.datastax.spark.connector._
import com.datastax.spark.connector.rdd._

import com.github.nscala_time.time.Imports._

package scrubjay {

  object Derivations {
    def ExpandTimeRange(sc: SparkContext, ds: DataSource): Option[DataSource] = {

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
