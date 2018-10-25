// Copyright 2018 Lawrence Livermore National Security, LLC and other
// ScrubJay Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: (Apache-2.0 OR MIT)

import scrubjay._
import com.datastax.spark.connector.CassandraRow

def hdfs_to_cassandra(hdfsFile: String, keyspace: String, table: String) {

    sc.setLocalProperty("spark.cassandra.output.concurrent.writes", "4")

    val mg2 = sc.textFile(hdfsFile)
    val header = mg2.first.split(",")
    val rest = mg2.zipWithIndex.filter(_._2 > 0).map(_._1)
    val parsed = rest.map(r => CassandraRow.fromMap(header.zip(r.split(",", -1)).map{case (k, "") => (k, null); case b => b}.toMap))

    parsed.saveToCassandra(keyspace, table)
}
