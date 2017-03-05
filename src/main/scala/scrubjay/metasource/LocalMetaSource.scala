package scrubjay.metasource

import scrubjay.metabase.MetaEntry.metaEntryFromStrings

case class LocalMetaSource(metaNames: Seq[(String, String, String, String)]) extends MetaSourceID {
  def realize: MetaSource = {
    metaNames.map(rawMetaEntry =>
      (rawMetaEntry._1, metaEntryFromStrings(rawMetaEntry._2, rawMetaEntry._3, rawMetaEntry._4))
    ).toMap
  }
}
