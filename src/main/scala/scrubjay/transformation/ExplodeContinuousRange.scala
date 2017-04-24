package scrubjay.transformation

import scrubjay.dataset.DatasetID

/*
case class ExplodeContinuousRange(dsID: DatasetID, column: String, period: Double)
  extends DatasetID(Seq(dsID)) {

  override lazy val isValid: Boolean = dsID.realize.schema(column).metadata.getString("dimension") match {
    case _ => false
  }

  def realize: ScrubJayRDD = {

    val ds = dsID.realize

    val rdd: RDD[DataRow] = {

      // Derivation function for flatMap returns a sequence of DataRows
      def derivation(row: DataRow, col: String): Seq[DataRow] = {

        // Get lists to explode
        val valueToExplode = row(col)
        val explodedValues: Iterator[Units[_]] = valueToExplode.asInstanceOf[ContinuousRange[Double]].explode(period)
        val rowWithoutColumn: Map[String, Units[_]] = row.filterNot(kv => column == kv._1)

        explodedValues.map(newValue => rowWithoutColumn ++ Map(newColumn -> newValue)).toSeq
      }

      // Create the derived dataset
      ds.flatMap(row => derivation(row, column))
    }

    new ScrubJayRDD(rdd)
  }
}
*/
