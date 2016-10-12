package scrubjay.derivation

import scrubjay.datasource.DataSource

abstract class Joiner(val ds1: DataSource, val ds2: DataSource) {

  def apply: Option[DataSource] = {
    if (isValid)
      Some(derive)
    else
      None
  }

  def isValid: Boolean

  def derive: DataSource

}
