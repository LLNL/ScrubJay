package scrubjay.transformation

import org.apache.spark.rdd.RDD
import scrubjay.metasource._
import scrubjay.datasource._
import scrubjay.metabase.MetaEntry
import scrubjay.units.Units

/*
class TransformColumn(dsID: DataSourceID, column: String, fn: Units[_] => Units[_], newMetaEntry: MetaEntry)
  extends DataSourceID(Seq(dsID))(Seq(column, fn, newMetaEntry)) {

  val isValid: Boolean = dsID.metaSource.columns contains column

  val metaSource: MetaSource = dsID.metaSource.withMetaEntries(Map(column -> newMetaEntry), overwrite = true)

  def realize: ScrubJayRDD = {

    val ds = dsID.realize

    val rdd: RDD[DataRow] = {
      ds.map(row => {
        row ++ Map(column -> fn(row(column)))
      })
    }

    new ScrubJayRDD(rdd)
  }
}

object TransformColumn {
  def apply(dsID: DataSourceID, column: String, fn: Units[_] => Units[_], newMetaEntry: MetaEntry): Option[TransformColumn] = {
    val derivedID = new TransformColumn(dsID, column, fn, newMetaEntry)
    if (derivedID.isValid)
      Some(derivedID)
    else
      None
  }
}
*/
