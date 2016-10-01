package scrubjay.units

import scala.language.implicitConversions

object ConversionHelpers {

  // Implicit converters for Any to types accepted by Units (e.g. Double)

  implicit def any2Double(a: Any): Double = a match {
    case n: Number => n.doubleValue
    case s: String => s.toDouble
    case _ => throw new RuntimeException(s"Cannot cast $a to Double!")
  }

  implicit def any2Int(a: Any): Int = a match {
    case n: Number => n.intValue
    case s: String => s.toInt
    case _ => throw new RuntimeException(s"Cannot cast $a to Int!")
  }

}
