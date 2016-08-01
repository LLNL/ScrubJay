package scrubjay.derivation

import scrubjay._
import scrubjay.meta._
import scrubjay.units._
import scrubjay.datasource._

import scala.reflect._
import org.apache.spark.rdd.RDD

/*
 * ExpandedIdentifierList
 *
 * Requirements: 
 *  1. A single DataSource to derive from
 *  2. A set of user-specified columns, all of which are UnitList[Identifier[_]]
 *
 * Derivation:
 *  For every row with a list of identifiers <a1, a2, identifiers [i1, i2, i3]>,
 *  creates a new row with identical attributes <a1, a2, i1>, <a1, a2, i2>, etc ...
 */

class ExpandIdentifierList(metaOntology: MetaBase,
                           ds: DataSource,
                           columns: List[String]) extends DerivedDataSource(metaOntology) {

  // Implementations of abstract members
  val defined: Boolean = columns.map(ds.metaEntryMap(_)).forall(_.units.tag == classTag[UnitList[_]])
  val metaEntryMap: MetaMap = ds.metaEntryMap.map {
    case (c, m) if columns.contains(c) =>
      (c + "_expanded", m.copy(units = m.units.children.head.asInstanceOf[MetaUnits]))
    case x => x
  }

  // rdd derivation defined here
  lazy val rdd: RDD[DataRow] = {

    // For multiple expansion columns, expand into the cartesian product
    def cartesianProduct[T](xss: List[List[T]]): List[List[T]] = xss match {
      case Nil => List(Nil)
      case h :: t => for(xh <- h; xt <- cartesianProduct(t)) yield xh :: xt
    }

    // Derivation function for flatMap returns a sequence of DataRows
    def derivation(row: DataRow, cols: List[String]): Seq[DataRow] = {

      val vals =
        row.filter{case (k, v) => cols.contains(k)}
        .map{
          case (k, ul: UnitList[_]) => ul.v.map{case u: Units[_] => (k + "_expanded", u)}
          case (k, v) => throw new RuntimeException(s"Runtime type mismatch: \nexpected: UnitList[_]\nvalue: $v")
        }
        .toList

      val combinations = cartesianProduct(vals)

      for (combination <- combinations) yield {
        (row ++ Map(combination:_*)).filterNot{case (k, v) => cols.contains(k)}
      }
    }

    // Create the derived dataset
    ds.rdd.flatMap(row =>
        derivation(row, columns))
  }
}

object ExpandIdentifierList {
  implicit class ScrubJaySession_ExpandedNodeList(sjs: ScrubJaySession) {
    def deriveExpandedNodeList(ds: DataSource, cols: List[String]): ExpandIdentifierList = {
      new ExpandIdentifierList(sjs.metaOntology, ds, cols)
    }
  }
}
