package scrubjay


import scrubjay.metabase._

package object metasource {

  type MetaSource = Map[String, MetaEntry]

  implicit class MetaSourceImplicits(metaSource: MetaSource) extends Serializable {

    val columns: Seq[String] = metaSource.keys.toSeq

    def columnForEntry(me: MetaEntry): Option[String] = {
      metaSource.map(_.swap).get(me)
    }

    def filterEntries(cond: MetaEntry => Boolean): MetaSource = {
      metaSource.filter { case (_, me) => cond(me) }
    }

    def withMetaEntries(newMetaEntryMap: MetaSource, overwrite: Boolean = false): MetaSource = {
      if (overwrite) {
        metaSource ++ newMetaEntryMap
      }
      else {
        val newEntries = newMetaEntryMap.filterNot(entry => metaSource.keySet.contains(entry._1))
        metaSource ++ newEntries
      }
    }

    def withColumns(newColumns: Seq[String], overwrite: Boolean = false): MetaSource = {
      if (overwrite) {
        metaSource ++ newColumns.map(_ -> UNKNOWN_META_ENTRY)
      }
      else {
        val knownEntries = metaSource.filter { case (k, _) => newColumns.contains(k) }
        val newEntries = newColumns.filterNot(metaSource.keySet.contains)
        knownEntries ++ newEntries.map(_ -> UNKNOWN_META_ENTRY)
      }
    }

    def withoutColumns(oldColumns: Seq[String]): MetaSource = {
      metaSource.filterNot { case (k, _) => oldColumns.contains(k) }
    }

  }
}
