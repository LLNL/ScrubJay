package scrubjay.transformation

import scrubjay.metabase._
import scrubjay.metasource._
import scrubjay.datasource._

import org.apache.spark.rdd.RDD


/*
 *  Valid resampling queries:
 *    GET (flops) PER (job)
 *    GET (job, flops) PER (time = Seconds(1) FROM time = 10:00 TO time = 11:00)
 */

case class Resample(dsID: DataSourceID, columns: Seq[String])
  extends DataSourceID(dsID) {

  def isValid: Boolean = ???

  val metaSource: MetaSource = dsID.metaSource

  def realize: ScrubJayRDD = ???

  /*
  {

    val ds = dsID.realize

    val rdd: RDD[DataRow] = {

      val reducer = metaEntry.units.unitsTag.reduce _

      ds.map(row => {
        val mergedVal = reducer(columns.map(row))
        row.filterNot{case (k, _) => columns.contains(k)} ++ Map(newColumn -> mergedVal)
      })
    }

    new ScrubJayRDD(rdd)
  }
  */
}
