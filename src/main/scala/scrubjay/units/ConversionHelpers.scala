package scrubjay.units

import scala.language.implicitConversions

object ConversionHelpers {

  // Implicit converters for Any to types accepted by units (e.g. Double)
  implicit def any2Double(a: Any): Double = a match {
    case n: Number => n.doubleValue
    case s: String => s.toDouble
    case _ => throw new RuntimeException(s"Cannot cast $a to Double!")
  }

}
