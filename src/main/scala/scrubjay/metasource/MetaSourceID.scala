package scrubjay.metasource

abstract class MetaSourceID {
  def realize: MetaSource
}

object MetaSourceID {
  def empty: MetaSourceID =  LocalMetaSource(Seq[(String, String, String, String, String)]())
}