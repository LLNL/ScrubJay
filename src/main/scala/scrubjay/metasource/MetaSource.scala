package scrubjay.metasource

import scrubjay.metabase.MetaDescriptor.MetaDimension
import scrubjay.metabase._

import scrubjay.util._

class MetaSource(val metaEntryMap: MetaEntryMap) extends Serializable {

  val columns: Seq[String] = metaEntryMap.keys.toSeq

  def columnForEntry(me: MetaEntry): Option[String] = {
    metaEntryMap.map(_.swap).get(me)
  }

  def filterEntries(cond: MetaEntry => Boolean): MetaEntryMap = {
    metaEntryMap.filter{case (_, me) => cond(me)}
  }

  def withMetaEntries(newMetaEntryMap: MetaEntryMap, overwrite: Boolean = false): MetaSource = {
    if (overwrite) {
      new MetaSource(metaEntryMap ++ newMetaEntryMap)
    }
    else {
      val newEntries = newMetaEntryMap.filterNot(entry => metaEntryMap.keySet.contains(entry._1))
      new MetaSource(metaEntryMap ++ newEntries)
    }
  }

  def withColumns(newColumns: Seq[String], overwrite: Boolean = false): MetaSource = {
    if (overwrite) {
      new MetaSource(metaEntryMap ++ newColumns.map(_ -> UNKNOWN_META_ENTRY))
    }
    else {
      val knownEntries = metaEntryMap.filter{case (k, _) => newColumns.contains(k)}
      val newEntries = newColumns.filterNot(metaEntryMap.keySet.contains)
      new MetaSource(knownEntries ++ newEntries.map(_ -> UNKNOWN_META_ENTRY))
    }
  }

  def withoutColumns(oldColumns: Seq[String]): MetaSource = {
    new MetaSource(metaEntryMap.filterNot { case (k, _) => oldColumns.contains(k) } )
  }

}

object MetaSource {

  def empty: MetaSource = {
    new MetaSource(Map[String, MetaEntry]())
  }

  def commonMetaEntries(ms1: MetaSource, ms2: MetaSource): Set[MetaEntry] = {
    ms1.metaEntryMap.values.toSet intersect ms2.metaEntryMap.values.toSet
  }

  def commonDimensionEntries(ms1: MetaSource, ms2: MetaSource): Seq[(MetaDimension, MetaEntry, MetaEntry)] = {

    val ms1Entries = ms1.metaEntryMap.values.filter(_.dimension != GlobalMetaBase.DIMENSION_UNKNOWN).map(me => (1, me))
    val ms2Entries = ms2.metaEntryMap.values.filter(_.dimension != GlobalMetaBase.DIMENSION_UNKNOWN).map(me => (2, me))

    (ms1Entries ++ ms2Entries).groupBy(_._2.dimension)
      .flatMap(kv => {
        val ms1d = kv._2.filter(_._1 == 1).map(_._2).toSeq
        val ms2d = kv._2.filter(_._1 == 2).map(_._2).toSeq
        for (ms1de <- ms1d; ms2de <- ms2d) yield (kv._1, ms1de, ms2de)
      }).toSeq

  }

}
