package scrubjay.datasource

import org.apache.spark.{Partition, TaskContext}
import scrubjay.metasource._
import org.apache.spark.rdd.RDD
import scrubjay.objectbase._
import scrubjay.units.Units


class ScrubJayRDD(fromRdd: ParsedRDD) extends RDD[DataRow](fromRdd) {

  // TODO: Derivation Provenance

  // Original?

  // Derived? What derivations? Arguments?

  // Save this to ScrubJay's objectbase
  def saveToObjectBase: Unit = {

    // If this is an original object, save it as such

    // If it is a derived object, compute the hash
    //   for its transformation path and store it as a
    //   derived object keyed by the hash.

    // RESEARCH QUESTION:
    //   How many sub-derivations to store? All of them?

    ???
  }

  def this(fromRawRdd: RawRDD, metaSource: MetaSource) {
    this(Units.rawRDDToUnitsRDD(fromRawRdd, metaSource))
  }

  override protected def getPartitions: Array[Partition] = {
    fromRdd.partitions
  }

  override def compute(split: Partition, context: TaskContext): Iterator[DataRow] = {
    fromRdd.compute(split, context)
  }

}

object ScrubJayRDD {
}
