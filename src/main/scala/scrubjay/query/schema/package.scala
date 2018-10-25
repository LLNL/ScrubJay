// Copyright 2018 Lawrence Livermore National Security, LLC and other
// ScrubJay Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: (Apache-2.0 OR MIT)

package scrubjay.query

package object schema {
  def wildMatch[T](s1: T, s2: Option[T]): Boolean = {
    s2.isEmpty || s1 == s2.get
  }
}
