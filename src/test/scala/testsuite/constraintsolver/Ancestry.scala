package testsuite.constraintsolver

import org.scalatest.FunSpec
import scrubjay.query.constraintsolver.ConstraintSolver._

class Ancestry extends FunSpec {

  // Constraint functions to satisfy
  lazy val parents: Constraint[Seq[String]] = memoize(args => {

    val person1 = args(0).as[String]
    val person2 = args(1).as[String]

    Seq(person1, person2) match {

      // parent(Great-Great-GrandPaw, Great-GrandPaw)
      case p @ Seq("Great-Great-GrandPaw", "Great-GrandPaw") => Seq(p)

      // parent(Great-GrandPaw, GrandPaw)
      case p @ Seq("Great-GrandPaw", "GrandPaw") => Seq(p)

      // parent(GrandPaw, Paw)
      case p @ Seq("GrandPaw", "Paw") => Seq(p)
      case p @ Seq("GrandPaw", "Maw") => Seq(p)

      // parent(Paw, Lief)
      case p @ Seq("Paw", "Lief") => Seq(p)
      case p @ Seq("Maw", "Lief") => Seq(p)

      // this is getting disgusting...
      case p @ Seq("GrandPaw", "Lief") => Seq(p)

      case _ => Seq.empty
    }

  })

  lazy val ancestors: Constraint[Seq[String]] = memoize(args => {

    val person1 = args(0).as[String]
    val person2 = args(1).as[String]

    // Necessary info for transitive case
    // parents(people, person2).flatMap(person2parent => person2parent.solutions
    // ancestors(person1, parents(people, person2))

    // Base case
    val parentSolution = parents(Seq(person1, person2))

    // ancestors(X,Y) := ancestors(X,Z), parents(Z,Y)
    val parent2Solutions = new ArgumentSpace(people, Seq(person2)).allSolutions(parents)
    val parent2Parents = parent2Solutions.map(_.arguments(0))
    val ancestorSolution = parent2Parents.flatMap(person2Parent => ancestors(Seq(person1, person2Parent)).map(_ :+ person2))

    ancestorSolution.toList ++ parentSolution
  })

  // Variables (possible inputs to Constraint functions)
  val people: Arguments = Seq(
    "Great-Great-GrandPaw",
    "Great-GrandPaw",
    "GrandPaw",
    "Paw",
    "Maw",
    "Lief"
  )

  describe("Ancestry Query") {

    val argSpace = new ArgumentSpace(people, people)

    val solutions = argSpace.allSolutions(ancestors)

    solutions.foreach(println)

    /*
    it("should have 10 solutions") {
      assert(solutions.length == 10)
    }

    it("should match ground truth") {
      assert(solutions == List(
        Seq(1, "Great-Great-GrandPaw", "Great-GrandPaw"),
        Seq(1, "Great-GrandPaw", "GrandPaw"),
        Seq(1, "GrandPaw", "Paw"),
        Seq(1, "Paw", "Lief"),
        Seq(2, "Great-Great-GrandPaw", "GrandPaw"),
        Seq(2, "Great-GrandPaw", "Paw"),
        Seq(2, "GrandPaw", "Lief"),
        Seq(3, "Great-Great-GrandPaw", "Paw"),
        Seq(3, "Great-GrandPaw", "Lief"),
        Seq(4, "Great-Great-GrandPaw", "Lief")
      ))
    }
    */

  }

}
