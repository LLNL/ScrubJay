name := "ScrubJay"

version := "1.0"

scalaVersion := "2.11.8"

scalacOptions := Seq("-feature", "-unchecked", "-deprecation")

val sparkVersion = "1.6.2"
val hadoopVersion = "2.6.2"
val cassandraVersion = "3.4.0"
val sparkCassandraConnectorVersion = "1.6.0"

// Spark
libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion
libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion
libraryDependencies += "org.apache.spark" %% "spark-mllib" % sparkVersion

// Cassandra
libraryDependencies += "com.datastax.spark" %% "spark-cassandra-connector" % sparkCassandraConnectorVersion excludeAll ExclusionRule(organization = "javax.servlet")

// Hadoop
libraryDependencies += "org.apache.hadoop" % "hadoop-common" % hadoopVersion excludeAll ExclusionRule(organization = "javax.servlet")

// Misc
libraryDependencies += "log4j" % "log4j" % "1.2.17"
libraryDependencies += "com.github.nscala-time" %% "nscala-time" % "2.12.0"
libraryDependencies += "com.github.tototoshi" %% "scala-csv" % "1.3.3"

// OscaR
resolvers += "Oscar Releases" at "http://artifactory.info.ucl.ac.be/artifactory/libs-release/"
libraryDependencies += "oscar" %% "oscar-cp" % "3.1.0"

// Fix dependency relocation for xml-apis
libraryDependencies += "xml-apis" % "xml-apis" % "1.0.b2"

// META-INF discarding for fat jar
assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}
