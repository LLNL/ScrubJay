package scrubjay.util

import com.github.nscala_time.time.Imports._

object util {
  def timeExpr[R](block: => R): R = {
    val t0 = System.nanoTime()
    val result = block    // call-by-name
    val t1 = System.nanoTime()
    println(s"Elapsed time: ${(t1-t0)/1000000000.0} seconds")
    result
  }

  def dateRange(start: DateTime, end: DateTime, step: Period): Iterator[DateTime] = {
    Iterator.iterate(start)(_.plus(step)).takeWhile(!_.isAfter(end))
  }

  implicit class Crossable[X](xs: Traversable[X]) {
    def cross[Y](ys: Traversable[Y]) = for { x <- xs; y <- ys } yield (x, y)
  }
}
