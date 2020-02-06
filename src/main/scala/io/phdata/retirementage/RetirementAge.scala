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

package io.phdata.retirementage

import io.phdata.retirementage.domain.GlobalConfig
import org.apache.spark.sql.SparkSession
import org.rogach.scallop.ScallopConf
import org.slf4j.LoggerFactory

import scala.io.Source

object RetirementAge {

  private val log = LoggerFactory.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .enableHiveSupport()
      .appName("retirement-age")
      .getOrCreate()

    val cliArgs            = new CliArgsParser(args)
    val yamlString: String = Source.fromFile(cliArgs.conf()).getLines().mkString("\n")

    val config = RetirementConfigParser.parse(yamlString)

    GlobalConfig.kuduMasters = config.kudu_masters

    val retirementReport =
      SparkDriver.retire(config, cliArgs.computeCounts(), cliArgs.dryRun(), cliArgs.undo())

    println("retirement report: {}", retirementReport)

    log.info(Reporting.toYaml(retirementReport))
  }

  private class CliArgsParser(args: Seq[String]) extends ScallopConf(args) {
    lazy val conf          = opt[String]("conf", required = true)
    lazy val computeCounts = opt[Boolean]("counts", required = false)
    lazy val dryRun        = opt[Boolean]("dry-run", required = false)
    lazy val undo          = opt[Boolean]("undo", required = false)

    verify()
  }

}
