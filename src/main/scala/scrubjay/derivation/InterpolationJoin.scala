package scrubjay.derivation

import scrubjay.units._
import scrubjay.metasource._
import scrubjay.datasource._
import scrubjay.metabase.MetaDescriptor.DimensionSpace

import scala.language.existentials
import org.apache.spark.rdd.RDD
import scrubjay.units.UnitsTag.DomainType

class InterpolationJoin(dso1: Option[DataSource], dso2: Option[DataSource], window: Double) extends Joiner(dso1, dso2) {

  val commonDimensions = MetaSource.commonDimensionEntries(ds1.metaSource, ds2.metaSource)
  val commonContinuousDimensions = commonDimensions.filter(d =>
    d._1.dimensionType == DimensionSpace.CONTINUOUS &&
    d._2.units.unitsTag.domainType == DomainType.POINT &&
    d._3.units.unitsTag.domainType == DomainType.POINT)
  val commonDiscreteDimensions = commonDimensions.filter(_._1.dimensionType == DimensionSpace.DISCRETE)

  // Single continuous axis for now
  def isValid = commonDimensions.nonEmpty && commonContinuousDimensions.length == 1

  def derive: DataSource = {

    new DataSource {

      override lazy val metaSource = ds2.metaSource.withMetaEntries(ds1.metaSource.metaEntryMap)

      override lazy val rdd: RDD[DataRow] = {

        val continuousDimColumn1 = commonContinuousDimensions.flatMap{case (d, me1, me2) => ds1.metaSource.columnForEntry(me1)}.head
        val continuousDimColumn2 = commonContinuousDimensions.flatMap{case (d, me1, me2) => ds2.metaSource.columnForEntry(me2)}.head

        val discreteDimColumns1 = commonDiscreteDimensions.flatMap{case (d, me1, me2) => ds1.metaSource.columnForEntry(me1)}
        val discreteDimColumns2 = commonDiscreteDimensions.flatMap{case (d, me1, me2) => ds2.metaSource.columnForEntry(me2)}

        val binnedRdd1 = ds1.rdd.flatMap(row => {
          val continuousVal = row(continuousDimColumn1).asInstanceOf[Continuous].asDouble
          // Bin by 2x window
          val key1 = discreteDimColumns1.map(row) :+ scala.math.floor(continuousVal/(2*window)).toInt
          // Overlapping bin by 1 window
          val key2 = discreteDimColumns1.map(row) :+ scala.math.floor((continuousVal+window)/(2*window)).toInt
          Seq((key1, row), (key2, row))
        })
        val binnedRdd2 = ds2.rdd.flatMap(row => {
          val continuousVal = row(continuousDimColumn2).asInstanceOf[Continuous].asDouble
          // Bin by 2x window
          val key1 = discreteDimColumns2.map(row) :+ scala.math.floor(continuousVal/(2*window)).toInt
          // Overlapping bin by 1 window
          val key2 = discreteDimColumns2.map(row) :+ scala.math.floor((continuousVal+window)/(2*window)).toInt
          Seq((key1, row), (key2, row))
        })

        // Co-group
        val cogrouped = binnedRdd1.cogroup(binnedRdd2)
          // Create 1 to N mapping from ds1 to ds2
          .flatMap{case (k, (l1, l2)) => l1.map(l1row => {
            // Filter out cells that are farther than `window` away
            val l1v = l1row(continuousDimColumn1).asInstanceOf[Continuous].asDouble
            (l1row, l2.filter(l2row => {
              val l2v = l2row(continuousDimColumn2).asInstanceOf[Continuous].asDouble
              Math.abs(l1v - l2v) < window
            // Convert to Set so we can avoid repeat rows
            }).toSet)
          })}
          .reduceByKey(_ ++ _) // Combine all ds2 rows that map to the same ds1 row, without repeat rows
          .mapValues(_.toSeq) // Convert back to Seq now that repeats are gone

        // Lift the heavies here
        val ds2MetaEntries = cogrouped.sparkContext.broadcast(ds2.metaSource.metaEntryMap)
        def projection(row: DataRow, keyColumn: String, mappedRows: Seq[DataRow], mappedKeyColumn: String): DataRow = {

          val observations = scala.collection.mutable.Map[String,(Seq[Double], Seq[Units[_]])]()

          // Add a single observation to the set of observations
          def addObservation(column: String, x: Double, y: Units[_]): Unit = {
            observations.get(column) match {
              case Some((xs, ys)) => observations.update(column, (xs :+ x, ys :+ y))
              case None => observations += (column -> (Seq(x), Seq(y)))
            }
          }

          for (mapRow <- mappedRows; mapElem <- mapRow.filterNot{case (k,v) => k == mappedKeyColumn}) {
            addObservation(mapElem._1, mapRow(mappedKeyColumn).asInstanceOf[Continuous].asDouble, mapElem._2)
          }

          val projectedValues = for (obs <- observations) yield {
            val unitsTag = ds2MetaEntries.value(obs._1).units.unitsTag
            val f = unitsTag.createInterpolator(obs._2._1, obs._2._2)
            obs._1 -> f(row(keyColumn).asInstanceOf[Continuous].asDouble)
          }

          row ++ projectedValues
        }

        cogrouped.map{case (k, l2) => projection(k, continuousDimColumn1, l2, continuousDimColumn2)}
      }
    }
  }
}
