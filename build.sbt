name := "ScrubJay"

version := "1.0"

//scalaVersion := "2.10.6"
scalaVersion := "2.11.8"

scalacOptions := Seq("-feature", "-unchecked", "-deprecation")

//val sparkVersion = "1.6.2"
val sparkVersion = "2.0.0"

//val sparkCassandraConnectorVersion = "1.6.0"
val sparkCassandraConnectorVersion = "2.0.0-M3"

val hadoopVersion = "2.6.2"
val cassandraVersion = "3.4.0"

// Spark
libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion
libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion
libraryDependencies += "org.apache.spark" %% "spark-mllib" % sparkVersion

// Cassandra
libraryDependencies += "com.datastax.spark" %% "spark-cassandra-connector" % sparkCassandraConnectorVersion excludeAll ExclusionRule(organization = "javax.servlet")

// Hadoop
libraryDependencies += "org.apache.hadoop" % "hadoop-common" % hadoopVersion excludeAll ExclusionRule(organization = "javax.servlet")

// Testing
libraryDependencies += "org.scalactic" %% "scalactic" % "3.0.0"
libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.0" % "test"

// Breeze
resolvers += "Sonatype Releases" at "https://oss.sonatype.org/content/repositories/releases/"
libraryDependencies += "org.scalanlp" %% "breeze" % "0.12"
libraryDependencies += "org.scalanlp" %% "breeze-natives" % "0.12"

// Msgpack (serialization)
libraryDependencies += "org.msgpack" %% "msgpack-scala" % "0.6.11"

// JSON serialization
libraryDependencies += "org.scala-lang.modules" %% "scala-pickling" % "0.10.1"
//libraryDependencies += "com.github.fommil" %% "spray-json-shapeless" % "1.3.0"

// Hashing
libraryDependencies += "com.roundeights" %% "hasher" % "1.2.0"

// Misc
libraryDependencies += "log4j" % "log4j" % "1.2.17"

// Fix dependency relocation for xml-apis
libraryDependencies += "xml-apis" % "xml-apis" % "1.0.b2"

// Disable parallel tests since each uses spark
parallelExecution in test := false

// Force scalaVersion
ivyScala := ivyScala.value map { _.copy(overrideScalaVersion = true) }

// META-INF discarding for fat jar
assemblyMergeStrategy in assembly := {
  case PathList("META-INF", _*) => MergeStrategy.discard
  case _ => MergeStrategy.first
}
