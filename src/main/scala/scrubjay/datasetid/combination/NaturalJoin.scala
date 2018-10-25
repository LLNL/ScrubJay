// Copyright 2018 Lawrence Livermore National Security, LLC and other
// ScrubJay Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: (Apache-2.0 OR MIT)

package scrubjay.datasetid.combination

import org.apache.spark.sql.DataFrame
import scrubjay.datasetid.DatasetID
import scrubjay.schema.ScrubJaySchema

case class NaturalJoin(override val dsID1: DatasetID, override val dsID2: DatasetID)
  extends Combination("NaturalJoin") {

  def joinedSchema: Option[ScrubJaySchema] = {
    dsID1.scrubJaySchema.joinSchema(dsID2.scrubJaySchema)
  }

  override def scrubJaySchemaFn: ScrubJaySchema = {
    joinedSchema
      .getOrElse(throw new RuntimeException("Invalid schema requested!"))
      .withGeneratedColumnNames
  }

  override def validFn: Boolean = {
    joinedSchema.isDefined &&
      dsID1.scrubJaySchema.joinableFields(dsID2.scrubJaySchema)
        // All joinable columns must be unordered, else must use interpolation join
        .forall(field => !scrubJaySchema.findDimensionOrDefault(field._1.dimension.name).ordered)
  }

  override def realize: DataFrame = {
    val df1 = dsID1.realize
    val df2 = dsID2.realize
    val commonColumns = dsID1.scrubJaySchema
      .joinableFields(dsID2.scrubJaySchema).map(_._1.name)

    df1.join(df2, commonColumns.toSeq)
  }
}
