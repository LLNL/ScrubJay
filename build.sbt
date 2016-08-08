name := "ScrubJay"

version := "1.0"

scalaVersion := "2.10.6"

scalacOptions := Seq("-feature", "-unchecked", "-deprecation")

val sparkVersion = "1.6.2"
val hadoopVersion = "2.6.2"
val cassandraVersion = "3.4.0"
val sparkCassandraConnectorVersion = "1.6.0"

// Spark
libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion
libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion
libraryDependencies += "org.apache.spark" %% "spark-mllib" % sparkVersion
libraryDependencies += "com.datastax.spark" %% "spark-cassandra-connector" % sparkCassandraConnectorVersion

// Hadoop
libraryDependencies += "org.apache.hadoop" % "hadoop-common" % hadoopVersion

// Misc
libraryDependencies += "com.github.nscala-time" %% "nscala-time" % "2.12.0"
libraryDependencies += "com.github.tototoshi" %% "scala-csv" % "1.3.3"

// META-INF discarding for uberjar
/*
assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}
*/