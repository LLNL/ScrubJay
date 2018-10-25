// Copyright 2018 Lawrence Livermore National Security, LLC and other
// ScrubJay Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: (Apache-2.0 OR MIT)

import scrubjay._

val startTime = 1465583600
val endTime = 1465583610
//val endTime = 1465584426

val apptrace_meta = createCassandraMetaSource(sc, "cab_dat_2016", "apptrace_meta")
val apptrace = sc.createCassandraDataSource("cab_dat_2016", "apptrace_before", apptrace_meta, whereConditions=Seq("\"Timestamp.g\" > " + startTime, "\"Timestamp.g\" < " + endTime))

val ipmi_meta = createCassandraMetaSource(sc, "cab_dat_2016", "ipmi_meta")
val ipmi = sc.createCassandraDataSource("cab_dat_2016", "ipmi_before", ipmi_meta, whereConditions=Seq("epoch > " + startTime, "epoch < " + endTime))

val ipmi_apptrace = ipmi.get.deriveInterpolationJoin(apptrace, 50)
