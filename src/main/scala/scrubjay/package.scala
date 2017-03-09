/**
 * Single entry point for ScrubJay API
 *
 * All functionality here is meant to be externally accessible via `import scrubjay.imports._`
 *
 */

import scrubjay.datasource._
import scrubjay.metabase._
import scrubjay.query._
import org.apache.spark._
import scrubjay.combination.{InterpolationJoin, NaturalJoin, RangeJoin}


package object scrubjay {

  /**
   * Standalone functions
   */

  def metaEntryFromStrings(relationType: String, dimension: String, units: String): MetaEntry = {
    scrubjay.metabase.MetaEntry.metaEntryFromStrings(relationType, dimension, units)
  }

  /**
   * SparkContext implicit functions
   */

  implicit class ScrubJaySessionImplicits(sc: SparkContext) {

    val metaBase: MetaBase = GlobalMetaBase.META_BASE
    var objectBase: Map[String, ScrubJayRDD] = Map.empty

    def loadAllObjects(): Unit = {
      objectBase = scrubjay.objectbase.ObjectBase.loadOriginalObjects(sc)
    }

  }

  /**
   * ScrubJayRDD implicit functions
   */

  implicit class DataSourceImplicits(dsID: DataSourceID) {

    /**
     * Derivations
     */

    def deriveMergeColumns(columns: Seq[String]): DataSourceID = {
      scrubjay.transformation.MergeColumns(dsID, columns)
    }

    def deriveCoreFrequency: DataSourceID = {
      scrubjay.transformation.CoreFrequency(dsID)
    }

    def deriveExplodeList(column: String): DataSourceID = {
      scrubjay.transformation.ExplodeDiscreteRange(dsID, column)
    }

    def deriveExplodeTimeSpan(column: String, period: Double): DataSourceID = {
      scrubjay.transformation.ExplodeContinuousRange(dsID, column, period)
    }

    def deriveNaturalJoin(dsID2: DataSourceID): DataSourceID = {
      NaturalJoin(dsID, dsID2)
    }

    def deriveInterpolationJoin(dsID2: DataSourceID, window: Double): DataSourceID = {
      InterpolationJoin(dsID, dsID2, window)
    }

    def deriveRangeJoin(dsID2: DataSourceID): DataSourceID = {
      RangeJoin(dsID, dsID2)
    }

    /**
     * Save formats
     */

    def saveToCassandra(keyspace: String, table: String): Unit = {
      scrubjay.datasource.CassandraDataSource.saveToCassandra(dsID, keyspace, table)
    }

    def createCassandraTable(keyspace: String, table: String, primaryKeys: Seq[String], clusterKeys: Seq[String]): Unit = {
      scrubjay.datasource.CassandraDataSource.createCassandraTable(dsID, keyspace, table, primaryKeys, clusterKeys)
    }

    def saveToCSV(filename: String,
                  wrapperChar: String = "\"",
                  delimiter: String = ",",
                  noneString: String = "null"): Unit = {
      scrubjay.datasource.CSVDataSource.saveToCSV(dsID, filename, wrapperChar, delimiter, noneString)
    }

    /**
      * Create a new LocalMetaSource by interactively choosing them from the GlobalMetaBase
      */
    /*
    def createMetaSourceInteractively: MetaSource = {

      createLocalMetaSource {
        {
          for (column <- ds.metaSource.columns) yield {

            // Get relation type
            Seq("domain", "value").foreach(relationType =>
              println(String.format("%-30s", relationType))
            )
            val relationType = scala.io.StdIn.readLine("Specify a relation type for column " + column + ": ")

            // Get meaning
            GlobalMetaBase.META_BASE.meaningBase.foreach(meaning =>
              println(String.format("%-30s %s", meaning._1, meaning._2.description))
            )
            val meaning = scala.io.StdIn.readLine("Specify a meaning for column " + column + ": ")

            // Get dimension
            GlobalMetaBase.META_BASE.dimensionBase.foreach(dimension =>
              println(String.format("%-30s %s", dimension._1, dimension._2.description))
            )
            val dimension = scala.io.StdIn.readLine("Specify a dimension for column " + column + ": ")

            // Get units
            GlobalMetaBase.META_BASE.unitsBase.foreach(units =>
              println(String.format("%-30s %s", units._1, units._2.description))
            )
            val units = scala.io.StdIn.readLine("Specify units for column " + column + ": ")

            column -> metaEntryFromStrings(relationType, meaning, dimension, units)
          }
        }.toMap
      }
    }
    */
  }
}
