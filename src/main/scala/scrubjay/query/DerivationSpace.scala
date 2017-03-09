package scrubjay.query

import scrubjay.metasource._
import scrubjay.datasource._
import scrubjay.transformation.{ExplodeContinuousRange, ExplodeDiscreteRange}

import gov.llnl.ConstraintSolver._


object DerivationSpace {
  // TODO: does object memoize correctly?

  val columnDerivations: Seq[(DataSourceID, String) => DataSourceID] = Seq(
    ExplodeDiscreteRange(_,_),
    ExplodeContinuousRange(_,_,60000) // FIXME: WINDOW SIZE
  )

  // Run a single transformation on all columns of this data source
  lazy val derivation: Constraint[DataSourceID] = memoize(args => {
    val dsID = args(0).as[DataSourceID]
    val derivation = args(1).as[(DataSourceID, String) => DataSourceID]

    val columns: Seq[String] = dsID.metaSource.columns

    columns.flatMap(column => derivation(dsID, column).asOption)
  })

  lazy val derivationChain: Constraint[DataSourceID] = memoize(args => {
    val dsID = args(0).as[DataSourceID]
    val derivations = args(1).as[Seq[(DataSourceID, String) => DataSourceID]]

    derivations match {

      // No derivations
      case Seq() => Seq()

      // One transformation
      case Seq(dv) => derivation(Seq(dsID, dv))

      // More than one, do a single transformation and pass that into a multi transformation with the rest of the derivations
      case n =>
        n.flatMap(dv =>
          derivation(Seq(dsID, dv)).flatMap(headSolution =>
            derivationChain(Seq(headSolution, n.filterNot(_ == dv)))))
    }
  })

  lazy val allDerivationChains: Constraint[DataSourceID] = memoize(args => {
    val dsID = args(0).as[DataSourceID]
    val derivationCombinations = 1.to(columnDerivations.length)
      .flatMap(c => columnDerivations.combinations(c))
    derivationCombinations.flatMap(c => derivationChain(Seq(dsID, c)))
  })
}
