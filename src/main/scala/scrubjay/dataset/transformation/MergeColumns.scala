package scrubjay.dataset.transformation

/*
case class MergeColumns(dsID: DatasetID, columns: Seq[String])
  extends DatasetID(dsID) {

  def newColumn: String = columns.mkString("_")
  def metaEntry: MetaEntry = dsID.metaSource(columns.head)

  def isValid: Boolean = columns.nonEmpty &&
    columns.forall(dsID.metaSource.columns contains _) &&
    columns.forall(c => dsID.metaSource(c).units == dsID.metaSource(columns.head).units)

  val metaSource: MetaSource = dsID.metaSource
    .withoutColumns(columns)
    .withMetaEntries(Map(newColumn -> metaEntry))

  def realize: ScrubJayRDD = {

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
}
*/
