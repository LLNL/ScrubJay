package scrubjay


package object meta {

  type MetaEntryMap = Map[String, MetaEntry]

  val UNKNOWN_META_ENTRY = MetaEntry.metaEntryFromStrings("unknown", "unknown", "identifier")

}
