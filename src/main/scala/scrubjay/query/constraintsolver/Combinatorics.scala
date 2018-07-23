package scrubjay.query.constraintsolver

import scala.reflect.ClassTag

object Combinatorics {

  // Cartesian of N Sequences
  def cartesianUntyped(ss: Seq[Seq[Any]]): Iterator[Seq[Any]] = ss match {
    case Seq() => Iterator()
    case Seq(a) => Iterator(a)
    case Seq(a, b) => {
      for (aa <- a.toIterator; bb <- b.toIterator) yield {
        Seq(aa, bb)
      }
    }
    case head +: tail => {
      for (aa <- head.toIterator; bb <- cartesianUntyped(tail)) yield {
        aa +: bb
      }
    }
  }

  def cartesian[T: ClassTag](ss: Seq[Seq[T]]): Iterator[Seq[T]] = {
    cartesianUntyped(ss).map(s => s.collect{case t: T => t})
  }
}
