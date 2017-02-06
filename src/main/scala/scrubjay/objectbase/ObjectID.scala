package scrubjay.objectbase

class ObjectID {

}

case class Derivation(input: Seq[ObjectID], function: String, arguments: Map[String, Any])

object ObjectID {
  case class Original(name: String) extends ObjectID
  case class Derived(derivation: Derivation) extends ObjectID
}
