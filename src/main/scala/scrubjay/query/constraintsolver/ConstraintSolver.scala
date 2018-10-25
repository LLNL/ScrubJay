// Copyright 2018 Lawrence Livermore National Security, LLC and other
// ScrubJay Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: (Apache-2.0 OR MIT)

package scrubjay.query.constraintsolver

import scala.language.implicitConversions

import scala.reflect.runtime.universe._

object ConstraintSolver {

  // Memoize function (avoid recalculating the same cases)
  def memoize[I, O](f: I => O): collection.Map[I, O] = new scala.collection.mutable.HashMap[I, O]() {
    override def apply(key: I): O = getOrElseUpdate(key, f(key))
  }

  // Cartesian of N Sequences
  def cartesian(ss: Seq[Arguments]): Iterator[Arguments] = ss match {
    case Seq() => Iterator()
    case Seq(a) => Iterator(a)
    case Seq(a, b) => {
      for (aa <- a.toIterator; bb <- b.toIterator) yield {
        Seq(aa, bb)
      }
    }
    case head +: tail => {
      for (aa <- head.toIterator; bb <- cartesian(tail)) yield {
        aa +: bb
      }
    }
  }

  // Implicitly wrap arguments with Arg, and unwrap
  implicit def t2Arg[T](t: T)(implicit tt: TypeTag[T]): Arg[T] = Arg(t)(tt)
  implicit def arg2T[T](arg: Arg[T]): T = arg.value
  implicit def ct2Arg[T](ct: Traversable[T])(implicit tt: TypeTag[T]): Seq[Arg[T]] = ct.map(Arg(_)(tt)).toSeq

  // A Constraint takes a set of Arguments and is either satisfied or not
  type Arguments = Seq[Arg[_]]
  type Constraint[T] = (Arguments) => Seq[T]

  case class Arg[T](value: T)(implicit tt: TypeTag[T]) {
    val tpe: _root_.scala.reflect.runtime.universe.Type = tt.tpe

    def as[A](implicit tt: TypeTag[A]): A = {
      if (tpe =:= tt.tpe)
        value.asInstanceOf[A]
      else
        throw new RuntimeException(s"Cannot cast $value to ${tt.tpe}, which has type $tpe")
    }
  }

  case class Solution[T](arguments: Arguments, solutions: Seq[T])

  // Space of all possible Arguments
  class ArgumentSpace(possibleArgs: Arguments*) {

    // Default space iterator, cartesian product of arguments, sorted in order of arguments
    def enumerate: Iterator[Arguments] = cartesian(possibleArgs.toSeq).map(args => Seq[Arg[_]](args: _*))

    def allSolutions[T](constraint: Constraint[T]): Iterator[Solution[T]] = {
      enumerate.flatMap(args => {
        constraint(args) match {
          case Seq() => None
          case solutions => Some(Solution(args, solutions))
        }
      })
    }
  }
}
