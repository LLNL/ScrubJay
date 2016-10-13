package scrubjay


package object metabase {

  type MetaEntryMap = Map[String, MetaEntry]

  val UNKNOWN_META_ENTRY = MetaEntry.metaEntryFromStrings("unknown", "unknown", "identifier")

}
