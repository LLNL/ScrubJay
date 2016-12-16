package scrubjay.derivation

import scrubjay.datasource.ScrubJayRDD

abstract class Transformer(val dso: Option[ScrubJayRDD]) extends Serializable {

  protected def isValid: Boolean
  protected def derive: ScrubJayRDD

  def apply: Option[ScrubJayRDD] = {
    if (dso.isDefined && isValid)
      Some(derive)
    else
      None
  }

  // FIXME: This is a bit unsafe:
  //   Anyone implementing a Transformer should be careful not to use ds anywhere that
  //   may force it to materialize non-lazily.
  protected lazy val ds: ScrubJayRDD = {
    if (dso.isDefined)
      dso.get
    else
      throw new RuntimeException("ds should only be accessed in methods or lazily!")
  }
}

