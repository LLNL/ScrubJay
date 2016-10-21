package scrubjay.derivation

import scrubjay.units._
import scrubjay.metasource._
import scrubjay.datasource._
import scrubjay.metabase.MetaDescriptor.DimensionType
import scrubjay.units.UnitsTag.DomainType
import breeze.interpolation.LinearInterpolator
import breeze.linalg.DenseVector
import breeze.math.Field
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

class InterpolationJoin(dso1: Option[DataSource], dso2: Option[DataSource], window: Double) extends Joiner(dso1, dso2) {

  val validEntries = MetaSource.commonMetaEntries(ds1.metaSource, ds2.metaSource)
    .filter(me =>
      me.units.unitsTag.domainType == DomainType.POINT &&
        me.dimension.dimensionType == DimensionType.CONTINUOUS)
    .toSeq

  // Single axis for now
  def isValid = validEntries.length == 1

  def derive: DataSource = {

    new DataSource {

      // Implementations of abstract members
      override lazy val metaSource = ds2.metaSource.withMetaEntries(ds1.metaSource.metaEntryMap)

      // RDD derivation defined here
      override lazy val rdd: RDD[DataRow] = {

        val keyColumns1 = validEntries.flatMap(ds1.metaSource.columnForEntry).head
        val keyColumns2 = validEntries.flatMap(ds2.metaSource.columnForEntry).head

        // Key by all values in the current window and next
        val flooredRDD1 = ds1.rdd.keyBy(row => scala.math.floor(row(keyColumns1).asInstanceOf[Double]/window).toInt)
        val flooredRDD2 = ds2.rdd.keyBy(row => scala.math.floor(row(keyColumns2).asInstanceOf[Double]/window).toInt)

        // Key by all values in the current window and previous
        val ceilRDD1 = ds1.rdd.keyBy(row => scala.math.ceil(row(keyColumns1).asInstanceOf[Double]/window).toInt)
        val ceilRDD2 = ds2.rdd.keyBy(row => scala.math.ceil(row(keyColumns2).asInstanceOf[Double]/window).toInt)

        // Group each
        val floorGrouped = flooredRDD1.cogroup(flooredRDD2)
        val ceilGrouped = ceilRDD1.cogroup(ceilRDD2)

        // Create 1 to N mapping from ds1 to ds2 for each
        val floorMapped = floorGrouped.flatMap{case (k, (l1, l2)) => l1.map(row => (row, l2))}
        val ceilMapped = ceilGrouped.flatMap{case (k, (l1, l2)) => l1.map(row => (row, l2))}

        // Co-group both window mappings and combine their results
        val grouped = floorMapped.cogroup(ceilMapped)
          .map{case (k, (f, c)) => (k, (f.flatten ++ c.flatten).toSeq)}

        // Lift the heavies here
        val ds2MetaEntries = grouped.sparkContext.broadcast(ds2.metaSource.metaEntryMap)
        def projection(row: DataRow, keyColumn: String, mappedRows: Seq[DataRow], mappedKeyColumn: String): DataRow = {

          val observations = scala.collection.mutable.Map[String,(Seq[Double], Seq[Units[_]])]()

          // Add a single observation to the set of observations
          def addObservation(column: String, x: Double, y: Units[_]): Unit = {
            observations.get(column) match {
              case Some((xs, ys)) => observations.update(column, (xs :+ x, ys :+ y))
              case None => observations += (column -> (Seq(x), Seq(y)))
            }
          }

          for (mapRow <- mappedRows; mapElem <- mapRow) {
            addObservation(mapElem._1, mapRow(mappedKeyColumn).asInstanceOf[Double], mapElem._2)
          }

          val projectedValues = for (obs <- observations) yield {
            val unitsTag = ds2MetaEntries.value(obs._1).units.unitsTag
            val typedValues = obs._2._2.map(unitsTag.extract)
            val f = unitsTag.createInterpolator(obs._2._1, typedValues)
            obs._1 -> f(row(keyColumn).value.asInstanceOf[Double])
          }

          row ++ projectedValues
        }

        grouped.map{case (k, l2) => projection(k, keyColumns1, l2, keyColumns2)}
      }
    }
  }
}
