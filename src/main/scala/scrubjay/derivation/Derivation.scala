package scrubjay.derivation


import scrubjay.ScrubJayRDD

import scala.pickling._
import scala.pickling.json._
import scala.pickling.Defaults._

class Derivation extends Serializable {
  def realize: ScrubJayRDD = throw new RuntimeException("Derivation should not be treated as a final class!")
}

object Derivation {
  def toJsonString(d: Derivation): String = d.pickle.value
  def fromJsonString(s: String): Derivation = functions.unpickle[Derivation](JSONPickle(s))
}

