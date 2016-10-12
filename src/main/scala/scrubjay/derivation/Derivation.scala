package scrubjay.derivation

import scrubjay.datasource.DataSource

abstract class Derivation(val ds: DataSource) extends Serializable {

  def apply: Option[DataSource] = {
    if (isValid)
      Some(derive)
    else
      None
  }

  def isValid: Boolean

  def derive: DataSource

}

