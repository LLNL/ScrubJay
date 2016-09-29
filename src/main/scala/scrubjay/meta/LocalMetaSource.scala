package scrubjay.meta

object LocalMetaSource {

  def createLocalMetaSource(metaEntryMap: MetaEntryMap): MetaSource = {
    new MetaSource(metaEntryMap)
  }

}
