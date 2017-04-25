package scrubjay.dataset.combination

import scrubjay.dataset.DatasetID

abstract class Combination extends DatasetID {
  val dsID1: DatasetID
  val dsID2: DatasetID
  override def dependencies: Seq[DatasetID] = Seq(dsID1, dsID2)
}
