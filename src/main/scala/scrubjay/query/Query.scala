package scrubjay.query

import scrubjay._
import scrubjay.meta._
import scrubjay.datasource._

import ConstraintSolver._


class Query(val sjs: ScrubJaySession,
            val dataSources: Seq[DataSource],
            val metaEntries: Seq[MetaEntry]) {

  def run: Iterator[DataSource] = {

    // Easy case: queried meta entries all contained in a single data source
    val dsContainsMetaEntries: Constraint[DataSource] = memoize(args => {
      val me: List[MetaEntry] = args.getAs[List[MetaEntry]](0)
      val ds: DataSource = args.getAs[DataSource](1)

      ds match {
        case dsSat if dsSat.containsMeta(me) => Some(dsSat)
        case _ => None
      }
    })

    val argSpace = new ArgumentSpace(Seq(metaEntries), dataSources)

    argSpace.allSolutions(dsContainsMetaEntries).flatMap(dsContainsMetaEntries(_))
  }

}

object Query {
  implicit class ScrubJaySession_Query(sjs: ScrubJaySession) {
    def runQuery(dataSources: Seq[DataSource], metaEntries: Seq[MetaEntry]): Iterator[DataSource] = {
      new Query(sjs, dataSources, metaEntries).run
    }
  }
}
