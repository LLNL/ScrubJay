package testsuite.constraintsolver

import org.scalatest.FunSpec
import scrubjay.query.constraintsolver.ConstraintSolver._

class NumberRiddle extends FunSpec {

  // Constraint functions to satisfy
  lazy val sumTo5: Constraint[Boolean] = memoize(args => {
    val x = args(0).as[Int]
    val y = args(1).as[Int]

    if (x + y == 5)
      Seq(true)
    else
      Seq.empty
  })

  lazy val diffOf1: Constraint[Boolean] = memoize(args => {
    val x = args(0).as[Int]
    val y = args(1).as[Int]

    if (y - x == 1)
      Seq(true)
    else
      Seq.empty
  })

  lazy val allConstraints: Constraint[(Boolean, Boolean)] = memoize(args => {
    sumTo5(args) zip diffOf1(args)
  })

  // Variables (possible inputs to Constraint functions)
  val x = Seq(1,2,3,4,5)
  val y = Seq(1,2,3,4,5)

  // ArgumentSpace and Seq[Constraint] makes the complete set of Rules
  val argSpace = new ArgumentSpace(x, y)

  describe("Number Riddle") {

    val solutions = argSpace.allSolutions(allConstraints)

    solutions.foreach(println)

    /*
    it("should have a solution") {
      assert(solution.isDefined)
    }

    it("should match ground truth") {
      assert(solution.get == Arguments(2,3))
    }
    */

  }
}
