package scrubjay.metasource

import scrubjay.metabase._
import scrubjay.metabase.MetaDescriptor.MetaDimension

object MetaSource {

  def empty: MetaSource = {
    Map.empty
  }

  def commonMetaEntries(ms1: MetaSource, ms2: MetaSource): Set[MetaEntry] = {
    ms1.values.toSet intersect ms2.values.toSet
  }

  def commonDimensionEntries(ms1: MetaSource, ms2: MetaSource): Seq[(MetaDimension, MetaEntry, MetaEntry)] = {

    val ms1Entries = ms1.values.filter(_.dimension != GlobalMetaBase.DIMENSION_UNKNOWN).map(me => (1, me))
    val ms2Entries = ms2.values.filter(_.dimension != GlobalMetaBase.DIMENSION_UNKNOWN).map(me => (2, me))

    (ms1Entries ++ ms2Entries).groupBy(_._2.dimension)
      .flatMap(kv => {
        val ms1d = kv._2.filter(_._1 == 1).map(_._2).toSeq
        val ms2d = kv._2.filter(_._1 == 2).map(_._2).toSeq
        for (ms1de <- ms1d; ms2de <- ms2d) yield (kv._1, ms1de, ms2de)
      }).toSeq

  }


}
