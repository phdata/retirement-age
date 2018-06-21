/* Copyright 2018 phData Inc. */

package io.phdata.retirementage.loadgen

import org.rogach.scallop.ScallopConf

class LoadGeneratorConfig(args: Array[String]) extends ScallopConf(args) {
  val factCount = opt[Int]("fact-count", required = true)
  val dimensionCount = opt[Int]("dimension-count", required = true)
  val subDimensionCount = opt[Int]("subdimension-count", required = true)
  val databaseName = opt[String]("database-name", required = true)

  verify()
}
