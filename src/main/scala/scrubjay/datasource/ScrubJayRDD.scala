package scrubjay.datasource

import org.apache.spark.{Partition, TaskContext}
import scrubjay.metasource.MetaSource
import org.apache.spark.rdd.RDD
import scrubjay.units.Units


class ScrubJayRDD(fromRdd: ParsedRDD, val metaSource: MetaSource) extends RDD[DataRow](fromRdd) {

  def this(fromRawRdd: RawRDD, metaSource: MetaSource) {
    this(Units.rawRDDToUnitsRDD(fromRawRdd, metaSource.metaEntryMap), metaSource)
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
