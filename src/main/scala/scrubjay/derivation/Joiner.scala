package scrubjay.derivation

import scrubjay.datasource.ScrubJayRDD

abstract class Joiner(val dso1: Option[ScrubJayRDD], val dso2: Option[ScrubJayRDD]) extends Serializable {

  def apply: Option[ScrubJayRDD] = {
    if (dso1.isDefined && dso2.isDefined && isValid)
      Some(derive)
    else
      None
  }

  protected def isValid: Boolean

  // FIXME: This is a bit unsafe:
  //   Anyone implementing a Joiner should be careful not to use ds1 or ds2 anywhere that
  //   may force either to materialize non-lazily.
  protected lazy val ds1: ScrubJayRDD = {
    if (dso1.isDefined)
      dso1.get
    else
      throw new RuntimeException("ds1 should only be accessed in methods or lazily!")
  }
  protected lazy val ds2: ScrubJayRDD = {
    if (dso2.isDefined)
      dso2.get
    else
      throw new RuntimeException("ds2 should only be accessed in methods or lazily!")
  }

  protected def derive: ScrubJayRDD

}
