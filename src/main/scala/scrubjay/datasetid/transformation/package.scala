package scrubjay.datasetid

import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.types.{Metadata, MetadataBuilder, StructField, StructType}
import org.apache.spark.sql.{Column, DataFrame, SparkSession}

package object transformation {
  def withExpr(expr: Expression): Column = new Column(expr)

  implicit class StructFieldWithMetadata(structField: StructField) {
    def withMetadataEntries(newMetadata: Map[String, Any]): StructField = {

      val oldMetadata = new MetadataBuilder().withMetadata(structField.metadata)
      val withNewMetadata = newMetadata
        .foldLeft(oldMetadata){
          case (m: MetadataBuilder, (key: String, value: Any)) => value match {
            case null => m.putNull(key)
            case longValue: Long => m.putLong(key, longValue)
            case doubleValue: Double => m.putDouble(key, doubleValue)
            case booleanValue: Boolean => m.putBoolean(key, booleanValue)
            case stringValue: String => m.putString(key, stringValue)
            case metadataValue: Metadata => m.putMetadata(key, metadataValue)
            case longArrayValue: Array[Long] => m.putLongArray(key, longArrayValue)
            case stringArrayValue: Array[String] => m.putStringArray(key, stringArrayValue)
            case doubleArrayValue: Array[Double] => m.putDoubleArray(key, doubleArrayValue)
            case booleanArrayValue: Array[Boolean] => m.putBooleanArray(key, booleanArrayValue)
            case metadataArrayValue: Array[Metadata] => m.putMetadataArray(key, metadataArrayValue)
            case _ => throw new RuntimeException(s"Metadata type of $value not supported")
          }
        }

      structField.copy(metadata = withNewMetadata.build())
    }

    def withMetadata(newMetadata: Metadata): StructField = {

      val newNewMetadata = new MetadataBuilder()
        .withMetadata(structField.metadata)
        .withMetadata(newMetadata)
        .build()
      structField.copy(metadata = newNewMetadata)
    }

    def withoutMetadata(removeMetadata: String*): StructField = {
      val oldMetadata = new MetadataBuilder().withMetadata(structField.metadata)
      val withoutRemovedMetadata = removeMetadata
        .foldLeft(oldMetadata)((m: MetadataBuilder, s: String) => m.remove(s))
        .build()

      structField.copy(metadata = withoutRemovedMetadata)
    }
  }

  implicit class DataFrameWithMetadata(DF: DataFrame) {
    def withStructField(structField: StructField): DataFrame = {

      val oldSchema = DF.schema

      val oldFieldIndex = oldSchema.indexWhere(_.name == structField.name)

      val newSchema = StructType{
        if (oldFieldIndex < 0)
          oldSchema :+ structField
        else
          oldSchema.updated(oldFieldIndex, structField)
      }

      // FIXME: This probably kills the predicate pushdown...
      SparkSession.builder().getOrCreate()
        .createDataFrame(DF.rdd, newSchema)
    }
  }
}
