package scrubjay.registrator

import com.esotericsoftware.kryo.Kryo
import org.joda.time.{DateTime, Interval}
import scrubjay.units._

class KryoRegistrator extends org.apache.spark.serializer.KryoRegistrator {

  override def registerClasses(kryo: Kryo): Unit = {
    // ScrubJay Units classes
    kryo.register(classOf[DateTimeSpan])
    kryo.register(classOf[DateTimeStamp])
    kryo.register(classOf[DegreesCelsius])
    kryo.register(classOf[OrderedContinuous])
    kryo.register(classOf[OrderedDiscrete])
    kryo.register(classOf[Seconds])
    kryo.register(classOf[Units[_]])
    kryo.register(classOf[UnitsList[_]])
    kryo.register(classOf[UnorderedDiscrete])

    // Classes used within Units classes
    kryo.register(classOf[DateTime])
    kryo.register(classOf[Interval])
  }
}
