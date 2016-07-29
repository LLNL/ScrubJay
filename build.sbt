name := "ScrubJay"

version := "1.0"

scalaVersion := "2.10.6"

scalacOptions := Seq("-feature", "-unchecked", "-deprecation")

val sparkVersion = "1.6.2"
val hadoopVersion = "2.6.2"
val cassandraVersion = "3.4.0"
val sparkCassandraConnectorVersion = "1.6.0"

// Misc
libraryDependencies += "com.github.nscala-time" %% "nscala-time" % "2.12.0"
libraryDependencies += "com.github.tototoshi" %% "scala-csv" % "1.3.3"

// Spark
libraryDependencies += "org.apache.spark" % "spark-core_2.10"   % sparkVersion
libraryDependencies += "org.apache.spark" % "spark-sql_2.10"    % sparkVersion
libraryDependencies += "org.apache.spark" % "spark-sql_2.10"    % sparkVersion
libraryDependencies += "org.apache.spark" % "spark-mllib_2.10"  % sparkVersion
libraryDependencies += "com.datastax.spark" % "spark-cassandra-connector_2.10" % sparkCassandraConnectorVersion

// Hadoop
libraryDependencies += "org.apache.hadoop" % "hadoop-common" % hadoopVersion

// META-INF discarding
mergeStrategy in assembly <<= (mergeStrategy in assembly) { (old) =>
   {
    case PathList("META-INF", xs @ _*) => MergeStrategy.discard
    case x => MergeStrategy.first
   }
}
