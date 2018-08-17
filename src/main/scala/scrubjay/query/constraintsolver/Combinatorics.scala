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

  // Given a composite object, return an iterator over all functions that can produce it
  //   from its decomposed subcomponents
  //     e.g. decompositions(cookie) = Bake(Mix(Flour, Sugar))
  def decompositions[T, D](composite: T,
                           decomposableTest: T => Boolean,
                           decompose: T => Seq[T],
                           applySingleComposition: T => (D => D)): Iterator[D => D] = {

    val compositions: Iterator[D => D] = {
      if (decomposableTest(composite)) {
        val subcomponents = decompose(composite)

        // Expand all subcomponents recursively
        val recursiveCase: Seq[Seq[D => D]] =
          subcomponents.map(s =>
            decompositions(s, decomposableTest, decompose, applySingleComposition).toSeq)

        val combinations: Iterator[D => D] = {
          // Cross product of expansions of subcomponents (ways to do first * ways to do second * ...)
          Combinatorics.cartesian(recursiveCase).map(c =>
            c.reduce((a: D => D, b: D => D) =>
              (dsID: D) => applySingleComposition(composite).apply(a.apply(b.apply(dsID)))))
        }

        Iterator(applySingleComposition(composite)) ++ combinations
      } else {
        Iterator.empty
      }
    }

    Iterator((d: D) => d) ++ compositions
  }
}
