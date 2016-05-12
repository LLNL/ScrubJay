import com.github.nscala_time.time.Imports._

package scrubjay {

  object util {
    def TimeExpr[R](block: => R): R = {
      val t0 = System.nanoTime()
      val result = block    // call-by-name
      val t1 = System.nanoTime()
      println(s"Elapsed time: ${(t1-t0)/1000000000.0} seconds")
      result
    }

    def DateRange(start: DateTime, end: DateTime, step: Period): Iterator[DateTime] = {
      Iterator.iterate(start)(_.plus(step)).takeWhile(!_.isAfter(end))
    }
  }
}
