package scrubjay.registrator

import com.datastax.spark.connector.CassandraRow
import com.esotericsoftware.kryo.Kryo
import com.twitter.chill.EnumerationSerializer
import org.joda.time.{DateTime, Interval}
import scrubjay.metabase.MetaDescriptor._
import scrubjay.metabase.MetaEntry
import scrubjay.units.UnitsTag.DomainType
import scrubjay.units._

class KryoRegistrator extends org.apache.spark.serializer.KryoRegistrator {

  override def registerClasses(kryo: Kryo): Unit = {

    // ScrubJay Units classes
    kryo.register(DateTimeSpan.getClass)
    kryo.register(DateTimeStamp.getClass)
    kryo.register(DegreesCelsius.getClass)
    kryo.register(OrderedContinuous.getClass)
    kryo.register(OrderedDiscrete.getClass)
    kryo.register(Seconds.getClass)
    kryo.register(Units.getClass)
    kryo.register(UnitsList.getClass)
    kryo.register(UnorderedDiscrete.getClass)

    // Classes used within Units classes
    kryo.register(classOf[DateTime])
    kryo.register(classOf[Interval])

    // ScrubJay meta classes
    kryo.register(classOf[MetaEntry])
    kryo.register(classOf[MetaDimension])
    kryo.register(classOf[MetaUnits])

    // Enumerations
    kryo.addDefaultSerializer(classOf[scala.Enumeration#Value], classOf[EnumerationSerializer])
    kryo.register(Class.forName("scala.Enumeration$Val"))
    kryo.register(classOf[scala.Enumeration#Value])
    kryo.register(MetaRelationType.getClass)
    kryo.register(DimensionSpace.getClass)
    kryo.register(DomainType.getClass)

    // Data rows
    kryo.register(classOf[CassandraRow])
    kryo.register(classOf[Map[String, Any]])
    kryo.register(classOf[Map[String, Units[_]]])
  }
}
