package scrubjay.derivation

import scrubjay.datasource._

class SemanticJoin(dso1: Option[ScrubJayRDD], dso2: Option[ScrubJayRDD]) extends Joiner(dso1, dso2) {

  override def isValid: Boolean = ???

  override def derive: ScrubJayRDD = ???

}
