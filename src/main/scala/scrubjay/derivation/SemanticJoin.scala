package scrubjay.derivation

import scrubjay.ScrubJaySession
import scrubjay.datasource.DataSource

object SemanticJoin {

  def deriveSemanticJoin(ds1: DataSource,
                         ds2: DataSource,
                         sjs: ScrubJaySession): Option[DataSource] = {

    // Find meta entries in ds1 and ds2 with common dimensions

    // Of those, determine the domains

    // Same dimension, "point" domain, categorical => natural join
    // Same dimension, "point" domain, quantitative => interpolation join
    // Same dimension, different domains "point" and "range" => lookup join

    None
  }

}
