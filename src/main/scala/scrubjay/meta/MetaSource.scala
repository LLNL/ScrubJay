package scrubjay.meta

class MetaSource(val metaEntryMap: MetaEntryMap) extends Serializable {
  val columns: Seq[String] = metaEntryMap.keys.toSeq
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
