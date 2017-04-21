package scrubjay.transformation

import org.apache.spark.rdd.RDD
import scrubjay.schema._
import scrubjay.dataset._

/*
class TransformColumn(dsID: DatasetID, column: String, fn: Units[_] => Units[_], newMetaEntry: MetaEntry)
  extends DatasetID(Seq(dsID))(Seq(column, fn, newMetaEntry)) {

  val isValid: Boolean = dsID.schema.columns contains column

  val schema: MetaSource = dsID.schema.withMetaEntries(Map(column -> newMetaEntry), overwrite = true)

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
  def apply(dsID: DatasetID, column: String, fn: Units[_] => Units[_], newMetaEntry: MetaEntry): Option[TransformColumn] = {
    val derivedID = new TransformColumn(dsID, column, fn, newMetaEntry)
    if (derivedID.isValid)
      Some(derivedID)
    else
      None
  }
}
*/
