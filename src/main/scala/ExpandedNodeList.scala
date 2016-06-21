import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

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

  class ExpandedNodeList(datasources: DataSource*) extends DerivedDataSource(datasources:_*) {

    // Derivation-specific variables for reuse
    val ds = datasources(0)
    val nodelist_meta_entry = MetaEntry(META_VALUE_NODE_LIST, META_UNITS_ID_LIST)
    val node_meta_entry = MetaEntry(META_VALUE_NODE, META_UNITS_ID)

    // Required input attributes and derived output attributes
    val RequiredMetaEntries = List(List(nodelist_meta_entry))
    val DerivedMetaEntries: MetaMap = Map(node_meta_entry -> "node")

    // Data derivation defined here
    lazy val Data: RDD[DataRow] = {

      // Derivation function for flatMap returns a sequence of DataRows
      def derivation(row: DataRow, nodelist_column: String, node_column: String): Seq[DataRow] = {

        // Get column value
        val nodelist_val = row(nodelist_column)

        // Create a row for each node in list
        nodelist_val match {
          case nodelist: List[_] => 
            for (node <- nodelist) yield { row + (node_column -> node) }
          case _ => // not a list, store single value
            Seq(row + (node_column -> nodelist_val))
        }
      }

      // Create the derived dataset
      ds.Data.flatMap(row => 
          derivation(row, ds.Meta(nodelist_meta_entry), DerivedMetaEntries(node_meta_entry)))
    }
  }
}
