package scrubjay.derivation

import scrubjay.datasource._

class SemanticJoin(dso1: Option[DataSource], dso2: Option[DataSource]) extends Joiner(dso1, dso2) {

  override def isValid: Boolean = ???

  override def derive: DataSource = ???

}
