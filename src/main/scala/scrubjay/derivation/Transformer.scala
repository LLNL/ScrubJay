package scrubjay.derivation

import scrubjay.datasource.DataSource

abstract class Transformer(val dso: Option[DataSource]) extends Serializable {

  def apply: Option[DataSource] = {
    if (dso.isDefined && isValid)
      Some(derive)
    else
      None
  }

  // FIXME: This is a bit unsafe:
  //   Anyone implementing a Transformer should be careful not to use ds anywhere that
  //   may force it to materialize non-lazily.
  protected lazy val ds = {
    if (dso.isDefined)
      dso.get
    else
      throw new RuntimeException("ds should only be accessed in methods or lazily!")
  }

  protected def isValid: Boolean

  protected def derive: DataSource

}

