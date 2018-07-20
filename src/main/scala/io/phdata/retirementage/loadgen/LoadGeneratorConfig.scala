/*
 * Copyright 2018 phData Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.phdata.retirementage.loadgen

import org.rogach.scallop.ScallopConf

class LoadGeneratorConfig(args: Array[String]) extends ScallopConf(args) {
  val factCount         = opt[Int]("fact-count", required = true, default = Some(0))
  val dimensionCount    = opt[Int]("dimension-count", required = true, default = Some(0))
  val subDimensionCount = opt[Int]("subdimension-count", required = true, default = Some(0))
  val databaseName      = opt[String]("database-name", required = true, default = Some("default"))

  val factName = opt[String]("fact-name", required = false, default = Some("factloadtest"))
  val dimName  = opt[String]("dim-name", required = false, default = Some("dimloadtest"))
  val subName  = opt[String]("subdim-name", required = false, default = Some("subloadtest"))

  verify()
}
