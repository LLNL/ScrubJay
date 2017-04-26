package scrubjay.datasetid

import scrubjay.util.writeStringToFile
import scrubjay.datasetid.transformation.{ExplodeDiscreteRange, Transformation}
import com.fasterxml.jackson.annotation.JsonSubTypes.Type
import com.fasterxml.jackson.annotation.{JsonIgnoreProperties, JsonSubTypes, JsonTypeInfo}
import com.fasterxml.jackson.core.{JsonGenerator, JsonParser}
import com.fasterxml.jackson.databind._
import com.fasterxml.jackson.databind.module.SimpleModule
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import com.roundeights.hasher.Implicits._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.parser.LegacyTypeStringParser
import org.apache.spark.sql.types.DataType
import org.json4s.jackson.JsonMethods
import scrubjay.datasetid.combination.Combination
import scrubjay.datasetid.original.{CSVDatasetID, OriginalDatasetID}

import scala.io.Source
import scala.util.Try

@JsonIgnoreProperties(
  value = Array("valid") // not sure why this gets populated
)
@JsonTypeInfo(
  use = JsonTypeInfo.Id.NAME,
  include = JsonTypeInfo.As.PROPERTY,
  property = "type"
)
@JsonSubTypes(Array(
  new Type(value = classOf[OriginalDatasetID], name = "OriginalDatasetID"),
  new Type(value = classOf[Transformation], name = "Transformation"),
  new Type(value = classOf[Combination], name = "Combination")
))
abstract class DatasetID extends Serializable {

  def scrubJaySchema: ScrubJaySchema

  def isValid: Boolean
  def dependencies: Seq[DatasetID]
  def realize: DataFrame
  def asOption: Option[DatasetID] = {
    if (isValid)
      Some(this)
    else
      None
  }
}

object DatasetID {

  private val objectMapper: ObjectMapper with ScalaObjectMapper = {
    val structTypeModule: SimpleModule = new SimpleModule()
    structTypeModule.addSerializer(classOf[SparkSchema], new SchemaSerializer())
    structTypeModule.addDeserializer(classOf[SparkSchema], new SchemaDeserializer())

    val m = new ObjectMapper with ScalaObjectMapper
    m.registerModule(DefaultScalaModule)
    m.registerModule(structTypeModule)
    m
  }

  def toHash(dsID: DatasetID): String = "h" + toJsonString(dsID).sha256.hex

  def toDotString(dsID: DatasetID): String = {

    val (nodes, edges) = toNodeEdgeTuple(dsID)

    val header = "digraph {"
    val nodeSection = nodes.map("\t" + _).mkString("\n")
    val edgeSection = edges.map("\t\t" + _).mkString("\n")
    val footer = "}"

    Seq(header, nodeSection, edgeSection, footer).mkString("\n")
  }

  def fromJsonFile(filename: String): DatasetID = {
    val fileContents = Source.fromFile(filename).getLines.mkString("\n")
    fromJsonString(fileContents)
  }

  def fromJsonString(json: String): DatasetID = {
    objectMapper.readValue[DatasetID](json, classOf[DatasetID])
  }

  def saveToJsonFile(dsID: DatasetID, filename: String): Unit = writeStringToFile(toJsonString(dsID), filename)

  def toJsonString(dsID: DatasetID): String = {
    objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(dsID)
  }

  def saveToDotFile(dsID: DatasetID, filename: String): Unit = writeStringToFile(toDotString(dsID), filename)

  private def toNodeEdgeTuple(dsID: DatasetID, parentName: Option[String] = None): (Seq[String], Seq[String]) = {

    val hash: String = toHash(dsID)

    // Create string of columns Node X Flops X Time, etc
    val columnString = dsID.realize.schema.fieldNames.mkString(" X ")

    // Graph node
    val style = dsID match {

      // Combined data sources
      // case _: NaturalJoin => "style=filled, fillcolor=\"forestgreen\", label=\"NaturalJoin\\n" + columnString + "\""
      // case _: InterpolationJoin => "style=filled, fillcolor=\"lime\", label=\"InterpolationJoin\\n" + columnString + "\""

      // Transformed data sources
      case _: ExplodeDiscreteRange => "style=filled, fillcolor=\"deepskyblue\", label=\"ExplodeDiscrete\\n" + columnString + "\""
      // case _: ExplodeContinuousRange => "style=filled, fillcolor=\"lightskyblue\", label=\"ExplodeContinuous\\n" + columnString + "\""

      // Original data sources
      case _: CSVDatasetID => "style=filled, fillcolor=\"darkorange\", label=\"CSV\\n" + columnString + "\""
      // case _: CassandraDatasetID => "style=filled, fillcolor=\"darkorange\", label=\"Cassandra\\n" + columnString + "\""
      // case _: LocalDatasetID => "style=filled, fillcolor=\"darkorange\", label=\"Local\\n" + columnString + "\""

      // Unknown
      case _ => "label='unknown'"
    }
    val node: String = hash + " [" + style + "]"

    // Graph edge
    val edge: Seq[String] = {
      if (parentName.isDefined)
        Seq(parentName.get + " -> " + hash + " [penwidth=2]")
      else
        Seq()
    }

    val (childNodes: Seq[String], childEdges: Seq[String]) = dsID.dependencies
      .map(toNodeEdgeTuple(_, Some(hash)))
      .fold((Seq.empty, Seq.empty))((a, b) => (a._1 ++ b._1, a._2 ++ b._2))

    (node +: childNodes, edge ++ childEdges)
  }

  /**
    * Serializer/Deserializer for SparkSchema (Spark DataFrame StructType)
    */

  class SchemaSerializer extends JsonSerializer[SparkSchema] {
    override def serialize(value: SparkSchema, gen: JsonGenerator, serializers: SerializerProvider): Unit = {

      import org.json4s.JsonAST.JValue
      import org.apache.spark.sql.scrubjayunits._

      val jValue: JValue = value.getJValue
      val jNode = JsonMethods.asJsonNode(jValue)
      gen.writeTree(jNode)
    }
  }

  class SchemaDeserializer extends JsonDeserializer[SparkSchema] {
    override def deserialize(p: JsonParser, ctxt: DeserializationContext): SparkSchema = {
      val json = p.readValueAsTree().toString
      Try(DataType.fromJson(json)).getOrElse(LegacyTypeStringParser.parse(json)) match {
        case t: SparkSchema => t
        case _ => throw new RuntimeException(s"Failed parsing SparkSchema: $json")
      }
    }
  }
}
