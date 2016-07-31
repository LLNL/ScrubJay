package scrubjay.derivation

import scrubjay._
import scrubjay.meta._
import scrubjay.units._
import scrubjay.datasource._
import scrubjay.util._

import scala.reflect._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.collection.immutable.Iterable

/*
 * ExpandedNodeList 
 * 
 * Requirements: 
 *  1. A single DataSource to derive from
 *  2. The column "Node List" in that DataSource
 *  3. The units "ID List" for that column
 *
 * Derivation:
 *  For every row with a node list <a1, a2, nodelist [1,2,3]>, creates
 *  a new row with identical attributes <a1, a2, 1>, <a1, a2, 2>, etc ...
 */

class ExpandList(metaOntology: MetaBase,
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

    def cartesianProduct[T](xss: List[List[T]]): List[List[T]] = xss match {
      case Nil => List(Nil)
      case h :: t => for(xh <- h; xt <- cartesianProduct(t)) yield xh :: xt
    }

    // Derivation function for flatMap returns a sequence of DataRows
    def derivation(row: DataRow, cols: List[String]): Seq[DataRow] = {

      val vals =
        row.filterKeys(cols.contains)
        .map{case (k, ul: UnitList[_]) => ul.v.map{case u: Units[_] => (k + "_expanded", u)}}
        .toList

      val combinations = cartesianProduct(vals)

      for (combination <- combinations) yield {
        (row ++ Map(combination:_*)).filterKeys(!cols.contains(_))
      }
    }

    // Create the derived dataset
    ds.rdd.flatMap(row =>
        derivation(row, columns))
  }
}

object ExpandList {
  implicit class ScrubJaySession_ExpandedNodeList(sjs: ScrubJaySession) {
    def deriveExpandedNodeList(ds: DataSource, cols: List[String]): ExpandList = {
      new ExpandList(sjs.metaOntology, ds, cols)
    }
  }
}
