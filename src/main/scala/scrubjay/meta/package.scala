package scrubjay

package object meta {
  type MetaEntryMap = Map[String, MetaEntry]
  val UNKNOWN_META_ENTRY = MetaEntry.fromStringTuple("unknown", "unknown", "identifier")
}
