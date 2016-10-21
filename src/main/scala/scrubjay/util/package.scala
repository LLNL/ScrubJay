package scrubjay

import scala.reflect.ClassTag
import scala.util.control.Exception.allCatch

package object util {

  def timeExpr[R](block: => R): R = {
    val t0 = System.nanoTime()
    val result = block    // call-by-name
    val t1 = System.nanoTime()
    println(s"Elapsed time: ${(t1-t0)/1000000000.0} seconds")
    result
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
    def ifDefinedThen[R](f: T => R): Option[R] = {
      o.fold(None: Option[R])(a => Some(f(a)))
    }
  }

}
