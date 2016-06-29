import org.apache.spark.rdd.RDD

import scrubjay.datasource._

import com.github.tototoshi.csv._

package scrubjay {

  object interactive {

    def interactiveMetaCreator(metaOntology: MetaOntology, columns: Seq[String]): MetaMap = {

      println("Entering ScrubJay interactive Meta creator")

      val metaList = metaOntology.ontologyMap.values.toList.zipWithIndex

      (for (column <- columns) yield {

        println("Available Meta descriptors:")
        metaList.foreach{case (descriptor, id) => println(id + ". " + descriptor)}

        print(s"Choose a Meta \'values\' entry for $column: ")
        val values_id = readInt()

        print(s"Choose a Meta \'units\' entry for $column: ")
        val units_id = readInt()

        (MetaEntry(metaList(values_id)._1, metaList(units_id)._1),
         column)
      }).toMap
    }

  }
}

