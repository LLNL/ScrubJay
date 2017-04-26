package scrubjay.query
//import scrubjay.datasetid.transformation.{ExplodeContinuousRange, ExplodeDiscreteRange}

/*

object DerivationSpace {
  // TODO: does object memoize correctly?

  val columnDerivations: Seq[(DatasetID, String) => DatasetID] = Seq(
    ExplodeDiscreteRange(_,_),
    ExplodeContinuousRange(_,_,60000) // FIXME: WINDOW SIZE
  )

  // Run a single transformation on all columns of this data source
  lazy val derivation: Constraint[DatasetID] = memoize(args => {
    val dsID = args(0).as[DatasetID]
    val derivation = args(1).as[(DatasetID, String) => DatasetID]

    val columns: Seq[String] = dsID.schema.columns

    columns.flatMap(column => derivation(dsID, column).asOption)
  })

  lazy val derivationChain: Constraint[DatasetID] = memoize(args => {
    val dsID = args(0).as[DatasetID]
    val derivations = args(1).as[Seq[(DatasetID, String) => DatasetID]]

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

  lazy val allDerivationChains: Constraint[DatasetID] = memoize(args => {
    val dsID = args(0).as[DatasetID]
    val derivationCombinations = 1.to(columnDerivations.length)
      .flatMap(c => columnDerivations.combinations(c))
    derivationCombinations.flatMap(c => derivationChain(Seq(dsID, c)))
  })
}
*/
