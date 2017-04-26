package scrubjay.datasetid.transformation

/*
class TransformColumn(dsID: DatasetID, column: String, fn: Units[_] => Units[_], newMetaEntry: MetaEntry)
  extends DatasetID(Seq(dsID))(Seq(column, fn, newMetaEntry)) {

  val isValid: Boolean = dsID.sparkSchema.columns contains column

  val sparkSchema: MetaSource = dsID.sparkSchema.withMetaEntries(Map(column -> newMetaEntry), overwrite = true)

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
