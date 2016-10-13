package scrubjay.metasource

import scrubjay.metabase._

object LocalMetaSource {

  def createLocalMetaSource(metaEntryMap: MetaEntryMap): MetaSource = {
    new MetaSource(metaEntryMap)
  }

}
