// Copyright 2018 Lawrence Livermore National Security, LLC and other
// ScrubJay Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: (Apache-2.0 OR MIT)

package scrubjay

import java.io.{BufferedWriter, File, FileWriter}

import scala.io.Source
import scala.reflect.ClassTag
import scala.util.control.Exception.allCatch

package object util {

  def timeExpr[R](block: => R): R = {
    val t0 = System.nanoTime()
    val result = block // call-by-name
    val t1 = System.nanoTime()
    println(s"Elapsed time: ${(t1 - t0) / 1000000000.0} seconds")
    result
  }

  def returnTime[R](block: => R): Double = {
    val t0 = System.nanoTime()
    val result = block
    val t1 = System.nanoTime()
    (t1 - t0) / 1000000000.0
  }

  def niceAttempt[T: ClassTag](block: => T): Option[T] = {

    val attempt = allCatch.toTry {
      block
    }

    attempt match {
      case exception: Throwable =>
        println(Console.RED + exception.getCause.getMessage)
        println(Console.RESET)
        None
      case result: T => Some(result)
    }
  }

  implicit class OptionIfDefinedThen[T](o: Option[T]) {
    def ifDefinedThen[R](f: T => R)(elseVal: Option[R] = None): Option[R] = {
      o.fold(elseVal)(a => Some(f(a)))
    }
  }

  def cartesianProduct[T](xss: Seq[Seq[T]]): Seq[Seq[T]] = xss match {
    case Nil => Seq(Nil)
    case h +: t => for (xh <- h; xt <- cartesianProduct(t)) yield xh +: xt
  }

  def writeStringToFile(text: String, filename: String): Unit = {
    val file = new File(filename)
    val bw = new BufferedWriter(new FileWriter(file))
    bw.write(text)
    bw.close()
  }

  def readFileToString(filename: String): String = {
    Source.fromFile(filename).getLines.mkString("\n")
  }

}
