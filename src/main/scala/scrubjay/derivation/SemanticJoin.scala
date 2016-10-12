package scrubjay.derivation

import scrubjay.datasource._

class SemanticJoin(ds1: DataSource, ds2: DataSource) extends Joiner(ds1, ds2) {
  override def isValid: Boolean = ???

  override def derive: DataSource = ???
}
