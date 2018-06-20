name := "ScrubJay"

version := "1.0"

scalaVersion := "2.11.8"

// Don't run tests in `sbt assembly`
test in assembly := {}

scalacOptions := Seq("-feature", "-unchecked", "-deprecation")

val sparkVersion = "2.3.0"

// Spark
libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion
libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion
libraryDependencies += "org.apache.spark" %% "spark-mllib" % sparkVersion

// Spark Cassandra Connector
libraryDependencies += "com.datastax.spark" %% "spark-cassandra-connector" % sparkVersion

// Hadoop
libraryDependencies += "org.apache.hadoop" % "hadoop-hdfs" % "2.7.0"
libraryDependencies += "org.apache.hadoop" % "hadoop-common" % "2.7.0"

// Testing
libraryDependencies += "org.scalactic" %% "scalactic" % "2.2.6"
libraryDependencies += "org.scalatest" %% "scalatest" % "2.2.6" % "test"

// Breeze
resolvers += "Sonatype Releases" at "https://oss.sonatype.org/content/repositories/releases/"
libraryDependencies += "org.scalanlp" %% "breeze" % "0.13.2"
libraryDependencies += "org.scalanlp" %% "breeze-natives" % "0.13.2"

// Msgpack (serialization)
libraryDependencies += "org.msgpack" %% "msgpack-scala" % "0.6.11"

// JSON serialization
libraryDependencies += "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.6.5"

// Hashing
libraryDependencies += "com.roundeights" %% "hasher" % "1.2.0"

// Log4j
libraryDependencies += "log4j" % "log4j" % "1.2.17"

// Plotly
libraryDependencies += "org.plotly-scala" %% "plotly-core" % "0.3.2"

// ScalaMeter (performance tests)
libraryDependencies += "com.storm-enroute" %% "scalameter" % "0.8.2" % "test"

// Fix dependency relocation for xml-apis
libraryDependencies += "xml-apis" % "xml-apis" % "1.3.04"

// Disable parallel tests since each uses spark
parallelExecution in test := false

// Force scalaVersion
ivyScala := ivyScala.value map { _.copy(overrideScalaVersion = true) }

// META-INF discarding for fat jar
assemblyMergeStrategy in assembly := {
  case PathList("META-INF", "services", "org.apache.hadoop.fs.FileSystem") => MergeStrategy.filterDistinctLines
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case PathList("javax", "servlet", xs @ _*) => MergeStrategy.first
  case _ => MergeStrategy.first
}
