package scrubjay.metasource

import scrubjay.meta.{MetaSource, _}

object LocalMetaSource {

  def createLocalMetaSource(metaEntryMap: MetaEntryMap): MetaSource = {
    new MetaSource(metaEntryMap)
  }

}
