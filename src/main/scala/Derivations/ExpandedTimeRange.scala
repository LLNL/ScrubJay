import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scrubjay._
import scrubjay.datasource._

package scrubjay {

  /*
   * ExpandedTimeRange 
   * 
   * Requirements: 
   *  1. A single DataSource to derive from
   *    2. The column "Start Time" and the column "End Time"
   *    OR
   *    2. The column "Start Time" and the column "Time Duration"
   *    OR
   *    2. The column "Time Duration" and the column "End Time"
   *    OR
   *    2. The column "Time Range" in that DataSource
   *
   *  3. The units "ID List" for that column
   *
   * Derivation:
   *  For every row with a node list <a1, a2, timerange [1,2,3]>, creates
   *  a new row with identical attributes <a1, a2, 1>, <a1, a2, 2>, etc ...

  object expandedTimeRange {

    class ExpandedTimeRange(metaOntology: MetaOntology,
                           ds: DataSource) extends DerivedDataSource(metaOntology) {

      // Meta entries used in this derivation
      final val starttime_meta_entry = MetaEntry(metaOntology.VALUE_START_TIME, metaOntology.UNITS_TIMESTAMP)
      final val endtime_meta_entry = MetaEntry(metaOntology.VALUE_END_TIME, metaOntology.UNITS_TIMESTAMP)
      final val duration_meta_entry = MetaEntry(metaOntology.VALUE_DURATION, metaOntology.UNITS_QUANTITY)
      final val timerange_meta_entry = MetaEntry(metaOntology.VALUE_TIME_RANGE, metaOntology.UNITS_TUPLE_TIMESTAMP_TIMESTAMP)

      final val timestamp_meta_entry = MetaEntry(metaOntology.VALUE_TIME_STAMP, metaOntology.UNITS_TUPLE_TIMESTAMP_TIMESTAMP)

      final val just_range = List(timerange_meta_entry)
      final val start_end = List(starttime_meta_entry, endtime_meta_entry)
      final val start_duration = List(starttime_meta_entry, duration_meta_entry)
      final val end_duration = List(endtime_meta_entry, duration_meta_entry)

      final val time_groups = List(just_range, start_end, start_duration, end_duration)

      val selected_time_group = time_groups.find(ds.containsMeta(_))

      // Implementations of abstract members
      val defined: Boolean = selected_time_group != None
      val metaMap: MetaMap = (ds.metaMap ++ Map(timestamp_meta_entry -> "Time Stamp")).filterNot(selected_time_group contains _._1)

      // rdd derivation defined here
      lazy val rdd: RDD[DataRow] = {

        // Create a row for each node in list
        val time_expander: DataRow => List[DataRow] = row => selected_time_groups match {
          case r  if r  == just_range     => None
          case se if se == start_end      => None
          case sd if sd == start_duration => None
          case ed if ed == end_duration   => None
        }

        // Derivation function for flatMap returns a sequence of DataRows
        def derivation(row: DataRow, selected_time_columns: List[MetaEntry]): Seq[DataRow] = {

          // Get column value
          val timevals = selected_time_columns.map(row)

        }

        // Create the derived dataset
        ds.rdd.flatMap(row => 
            derivation(row, ds.metaMap(timerange_meta_entry), metaMap(node_meta_entry)))
      }
    }

    implicit class ScrubJaySession_ExpandedTimeRange(sjs: ScrubJaySession) {
      def deriveExpandedTimeRange(ds: DataSource): ExpandedTimeRange = {
        new ExpandedTimeRange(sjs.metaOntology, ds)
      }
    }
  }
   */
}
