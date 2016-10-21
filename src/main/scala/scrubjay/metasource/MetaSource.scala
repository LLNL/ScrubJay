package scrubjay.metasource

import scrubjay.metabase.MetaDescriptor.MetaDimension
import scrubjay.metabase.{MetaEntry, _}

import scrubjay.util._

class MetaSource(val metaEntryMap: MetaEntryMap) extends Serializable {

  val columns: Seq[String] = metaEntryMap.keys.toSeq

  def columnForEntry(me: MetaEntry): Option[String] = {
    metaEntryMap.map(_.swap).get(me)
  }

  def filterEntries(cond: MetaEntry => Boolean): MetaEntryMap = {
    metaEntryMap.filter{case (k, me) => cond(me)}
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
      val knownEntries = metaEntryMap.filter{case (k, v) => newColumns.contains(k)}
      val newEntries = newColumns.filterNot(metaEntryMap.keySet.contains)
      new MetaSource(knownEntries ++ newEntries.map(_ -> UNKNOWN_META_ENTRY))
    }
  }

  def withoutColumns(oldColumns: Seq[String]): MetaSource = {
    new MetaSource(metaEntryMap.filter{case (k, v) => oldColumns.contains(k)})
  }

}

object MetaSource {

  def empty = {
    new MetaSource(Map[String, MetaEntry]())
  }

  def commonMetaEntries(ms1: MetaSource, ms2: MetaSource): Set[MetaEntry] = {
    ms1.metaEntryMap.values.toSet intersect ms2.metaEntryMap.values.toSet
  }

  def commonDimensionEntries(ms1: MetaSource, ms2: MetaSource): Map[MetaDimension, (MetaEntry, MetaEntry)] = {
    val ms1Dims = ms1.metaEntryMap.filter(_._2.dimension != GlobalMetaBase.DIMENSION_UNKNOWN).map{case (k, me) => me.dimension -> me}
    val ms2Dims = ms2.metaEntryMap.filter(_._2.dimension != GlobalMetaBase.DIMENSION_UNKNOWN).map{case (k, me) => me.dimension -> me}
    ms1Dims.flatMap{case (d, me1) => ms2Dims.get(d).ifDefinedThen(me2 => (d, (me1, me2)))}
  }

}
