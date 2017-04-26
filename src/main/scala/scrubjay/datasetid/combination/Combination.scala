package scrubjay.datasetid.combination

import scrubjay.datasetid.DatasetID

abstract class Combination extends DatasetID {
  val dsID1: DatasetID
  val dsID2: DatasetID
  override def dependencies: Seq[DatasetID] = Seq(dsID1, dsID2)
}
