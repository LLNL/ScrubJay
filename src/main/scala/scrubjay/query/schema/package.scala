package scrubjay.query

package object schema {
  def wildMatch[T](s1: T, s2: Option[T]): Boolean = {
    s2.isEmpty || s1 == s2.get
  }
}
