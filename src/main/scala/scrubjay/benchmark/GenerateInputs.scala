// Copyright 2018 Lawrence Livermore National Security, LLC and other
// ScrubJay Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: (Apache-2.0 OR MIT)

package scrubjay.benchmark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.scrubjayunits.ScrubJayLocalDateTime_String
import scrubjay.datasetid.original.LocalDatasetID
import scrubjay.datasetid.DatasetID
import scrubjay.dataspace.DimensionSpace
import scrubjay.schema.{ScrubJayDimensionSchema, ScrubJayColumnSchema, ScrubJaySchema, ScrubJayUnitsSchema}

import scala.util.Random

object GenerateInputs {

  def spark: SparkSession = SparkSession.builder().getOrCreate()

  val randGen = new Random(100)

  def dimensionSpace: DimensionSpace = {
    DimensionSpace(Array(
      ScrubJayDimensionSchema("time", ordered=true, continuous=true),
      ScrubJayDimensionSchema("node", ordered=false, continuous=false),
      ScrubJayDimensionSchema("flops", ordered = true, continuous = true),
      ScrubJayDimensionSchema("temperature", ordered=true, continuous=true)
    ))
  }

  val SJFieldNode =
    ScrubJayColumnSchema(
      domain=true,
      name="node",
      dimension=ScrubJayDimensionSchema("node"),
      units=ScrubJayUnitsSchema("identifier", "POINT", "nearest", "nearest", Map.empty))

  val SJFieldNodeList =
    ScrubJayColumnSchema(
      domain=true,
      name="nodelist",
      dimension=ScrubJayDimensionSchema("node"),
      units=ScrubJayUnitsSchema("ArrayString", "MULTIPOINT", "nearest", "nearest",
        Map("listUnits" -> SJFieldNode.units)))

  val SJFieldTimestamp =
    ScrubJayColumnSchema(
      domain=true,
      name="timestamp",
      dimension=ScrubJayDimensionSchema("time"),
      units=ScrubJayUnitsSchema("datetimestamp", "POINT", "average", "linear", Map.empty))

  val SJFieldTemperature =
    ScrubJayColumnSchema(
      domain=false,
      name="temperature",
      dimension=ScrubJayDimensionSchema("temperature"),
      units=ScrubJayUnitsSchema("temperature", "POINT", "average", "linear", Map.empty))

  val SJFieldFlops =
    ScrubJayColumnSchema(
      domain=false,
      name="flops",
      dimension=ScrubJayDimensionSchema("flops"),
      units=ScrubJayUnitsSchema("flops", "POINT", "average", "linear", Map.empty))


  val start: ScrubJayLocalDateTime_String = ScrubJayLocalDateTime_String.deserialize("1988-05-14 00:00:00")

  val timestampUDF: UserDefinedFunction = udf((i: Long) => {
    new ScrubJayLocalDateTime_String(start.value.plusNanos(i * 1000000000L))
  })

  val flopsUDF: UserDefinedFunction = udf(() => {
    randGen.nextInt(1000000)
  })

  val temperatureUDF: UserDefinedFunction = udf(() => {
    randGen.nextDouble()*100.0
  })

  def nodeXFlops(numRows: Long): DatasetID = {

    // Create nodes
    val df = spark.range(numRows).toDF("node")
      .withColumn("flops", flopsUDF())

    // Create ScrubJaySchema
    val sjSchema = new ScrubJaySchema(Set(
      SJFieldNode,
      SJFieldFlops
    ))

    LocalDatasetID(df, sjSchema)
  }

  def nodeXTemperature(numRows: Long): DatasetID = {

    // Create nodes
    val df = spark.range(numRows).toDF("node")
      .withColumn("temperature", temperatureUDF())

    // Create ScrubJaySchema
    val sjSchema = new ScrubJaySchema(Set(
      SJFieldNode,
      SJFieldTemperature
    ))

    LocalDatasetID(df, sjSchema)
  }

  def timeXTemp(numRows: Long): DatasetID = {

    // Generate dataframe
    val df = spark.range(numRows).toDF("timestamp")
      .withColumn("timestamp", timestampUDF(col("timestamp")))
      .withColumn("temperature", temperatureUDF())
      .cache

    // Create ScrubJaySchema
    val sjSchema = new ScrubJaySchema(Set(
      SJFieldTimestamp,
      SJFieldTemperature
    ))

    LocalDatasetID(df, sjSchema)
  }

  def timeXFlops(numRows: Long): DatasetID = {

    // Start time at birth
    val start: ScrubJayLocalDateTime_String = ScrubJayLocalDateTime_String.deserialize("1988-05-14 00:00:00")

    // Create timestamps
    val timestampUDF = udf((i: Long) => {
      new ScrubJayLocalDateTime_String(start.value.plusNanos(i * 1000000000L))
    })

    // Create flops
    val flopsUDF = udf(() => {
      randGen.nextInt(1000000)
    })

    // Generate dataframe
    val df = spark.range(numRows).toDF("timestamp")
      .withColumn("timestamp", timestampUDF(col("timestamp")))
      .withColumn("flops", flopsUDF())
      .cache

    // Create ScrubJaySchema
    val sjSchema = new ScrubJaySchema(Set(
      SJFieldTimestamp,
      SJFieldFlops
    ))

    LocalDatasetID(df, sjSchema)
  }
}
