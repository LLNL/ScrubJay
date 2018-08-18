package scrubjay.query.constraintsolver

import scrubjay.datasetid.DatasetID
import scrubjay.datasetid.original.CSVDatasetID

import scala.reflect.ClassTag

object Combinatorics {

  // Cartesian of N Sequences
  def cartesianUntyped(ss: Seq[Seq[Any]]): Iterator[Seq[Any]] = ss match {
    case Seq() => Iterator()
    case Seq(a) => a.map(i => Seq(i)).iterator
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

        // Cross product of expansions of subcomponents (ways to do first * ways to do second * ...)
        val combinations: Seq[D => D] =
          Combinatorics.cartesian(recursiveCase).toSeq.map(c =>
            c.foldLeft(applySingleComposition(composite))((b, f) => (d: D) => b.apply(f.apply(d))))

        combinations.iterator
      } else {
        Iterator.empty
      }
    }

    Iterator((d: D) => d) ++ compositions
  }
}
