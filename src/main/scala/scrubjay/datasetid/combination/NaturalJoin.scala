package scrubjay.datasetid.combination

import org.apache.spark.sql.DataFrame
import scrubjay.datasetid.DatasetID
import scrubjay.dataspace.DimensionSpace
import scrubjay.schema.ScrubJaySchema

case class NaturalJoin(override val dsID1: DatasetID, override val dsID2: DatasetID)
  extends Combination("NaturalJoin") {

  def joinedSchema(dimensionSpace: DimensionSpace): Option[ScrubJaySchema] = {
    dsID1.scrubJaySchema(dimensionSpace).joinSchema(dsID2.scrubJaySchema(dimensionSpace))
  }

  override def scrubJaySchema(dimensionSpace: DimensionSpace): ScrubJaySchema = {
    joinedSchema(dimensionSpace)
      .getOrElse(throw new RuntimeException("Invalid schema requested!"))
      .withGeneratedFieldNames
  }

  override def isValid(dimensionSpace: DimensionSpace): Boolean = {
    joinedSchema(dimensionSpace).isDefined &&
      dsID1.scrubJaySchema(dimensionSpace).joinableFields(dsID2.scrubJaySchema(dimensionSpace))
        // All joinable fields must be unordered, else must use interpolation join
        .forall(field => !dimensionSpace.findDimensionOrDefault(field._1.dimension).ordered)
  }

  override def realize(dimensionSpace: DimensionSpace): DataFrame = {
    val df1 = dsID1.realize(dimensionSpace)
    val df2 = dsID2.realize(dimensionSpace)
    val commonColumns = dsID1.scrubJaySchema(dimensionSpace)
      .joinableFields(dsID2.scrubJaySchema(dimensionSpace)).map(_._1.name)

    df1.join(df2, commonColumns.toSeq)
  }
}
