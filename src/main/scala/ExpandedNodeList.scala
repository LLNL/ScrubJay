import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scrubjay._
import scrubjay.datasource._

package scrubjay {

  /*
   * ExpandedNodeList 
   * 
   * Requirements: 
   *  1. An input DataSource to derive from
   *  2. The column "Node List" in that DataSource
   *  3. The units "ID List" for that column
   *
   * Derivation:
   *  For every row with a node list <a1, a2, nodelist [1,2,3]>, creates
   *  a new row with identical attributes <a1, a2, 1>, <a1, a2, 2>, etc ...
   */

  object expandedNodeList {

    class ExpandedNodeList(metaOntology: MetaOntology,
                           datasources: DataSource*) extends DerivedDataSource(metaOntology, datasources:_*) {

      // Derivation-specific variables for reuse
      val ds = datasources(0)
      val nodelist_meta_entry = MetaEntry(metaOntology.VALUE_NODE_LIST, metaOntology.UNITS_ID_LIST)
      val node_meta_entry = MetaEntry(metaOntology.VALUE_NODE, metaOntology.UNITS_ID)

      // Required input attributes and derived output attributes
      val requiredMetaEntries = List(List(nodelist_meta_entry))
      val metaMap: MetaMap = (datasources.map(_.metaMap).reduce(_ ++ _) ++ Map(node_meta_entry -> "node")) - nodelist_meta_entry

      // rdd derivation defined here
      lazy val rdd: RDD[DataRow] = {

        // Derivation function for flatMap returns a sequence of DataRows
        def derivation(row: DataRow, nodelist_column: String, node_column: String): Seq[DataRow] = {

          // Get column value
          val nodelist_val = row(nodelist_column)

          // Create a row for each node in list
          nodelist_val match {
            case nodelist: List[_] => 
              for (node <- nodelist) yield { row + (node_column -> node) - nodelist_column}
            case _ => // not a list, store single value
              Seq(row + (node_column -> nodelist_val))
          }
        }

        // Create the derived dataset
        ds.rdd.flatMap(row => 
            derivation(row, ds.metaMap(nodelist_meta_entry), metaMap(node_meta_entry)))
      }
    }

    implicit class ScrubJaySession_ExpandedNodeList(sjs: ScrubJaySession) {
      def deriveExpandedNodeList(ds: DataSource): ExpandedNodeList = {
        new ExpandedNodeList(sjs.metaOntology, ds)
      }
    }
  }
}
