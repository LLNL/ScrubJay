package scrubjay.dataset.transformation

import scala.language.existentials


/*
 *  Example resamplings:
 *
 *    GET (flops) PER (job)
 *    GET (flops, job) PER (time [period=Seconds(1)])
 *    GET (flops, job) PER (time [period=Seconds(1), start=10:00, end=11:00])
 *    TODO: GET (flops, job) PER (node, job)
 */

/*
case class ResampleMethod[T: Units](metaEntry: MetaEntry, start: Option[T], end: Option[T], period: Option[Double])

case class Resample(dsID: DatasetID, method: ResampleMethod[_])
  extends DatasetID {

  override val sparkSchema: MetaSource = dsID.sparkSchema

  override def isValid: Boolean = true

  override def realize: ScrubJayRDD = {
    val ds = dsID.realize

    // Key each row by the provided metaEntry
    val column = sparkSchema.columnForEntry(method.metaEntry).get

    var rdd = ds.rdd.keyBy(row => row(column).asInstanceOf[Continuous].asDouble)

    // If we have a start filter
    if (method.start.isDefined) {
      rdd = rdd.filter(
        _._1.asInstanceOf[Continuous].asDouble >=
          method.start.get.asInstanceOf[Continuous].asDouble)
    }

    // If we have an end filter
    if (method.end.isDefined) {
      rdd = rdd.filter(
        _._1.asInstanceOf[Continuous].asDouble >=
          method.end.get.asInstanceOf[Continuous].asDouble)
    }

    // If we have a period, transform the key into the period
    if (method.period.isDefined) {
      val periodDouble = method.period.asInstanceOf[Continuous].asDouble
      rdd = rdd.map{case (key, row) =>
        (key / periodDouble, row)
      }
    }

    // Reduce all other columns by the bin (aggregateByKey)
    val reducer = method.metaEntry.units.unitsTag.reduce _
    rdd = rdd.map{case (key, rows) => {
      val aggregatedRow = ???
      null
    }}

    new ScrubJayRDD(ds.rdd)
  }
}
*/
