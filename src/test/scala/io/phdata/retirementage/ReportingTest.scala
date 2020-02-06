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

import io.phdata.retirementage.domain.{DatasetReport, DatedTable, RetirementReport}
import org.scalatest.FunSuite

class ReportingTest extends FunSuite {
  test("write standard report") {
    val table = DatedTable("table1", "parquet", "the_date", 100, None, None, None)
    val results = RetirementReport(
      "default.table1",
      true,
      DatasetReport("hdfs://data/default/table1", Some(100)),
      Some(DatasetReport("hdfs://data/default/table1_ra", Some(90))),
      None
    )

    println(Reporting.toYaml(Seq(results)))
  }

  test("write nested report") {
    val report = List(
      RetirementReport("impala::pcb_rawsbx1.T_RCEX1P_3",
                       false,
                       DatasetReport("impala::pcb_rawsbx1.T_RCEX1P_3", None),
                       None,
                       None),
      RetirementReport("impala::pcb_rawsbx1.T_RCEX4P",
                       false,
                       DatasetReport("impala::pcb_rawsbx1.T_RCEX4P", None),
                       None,
                       None),
      RetirementReport("impala::pcb_rawsbx1.T_RCIVLP",
                       false,
                       DatasetReport("impala::pcb_rawsbx1.T_RCIVLP", None),
                       None,
                       None),
      RetirementReport("impala::pcb_rawsbx1.T_RCEXAP",
                       false,
                       DatasetReport("impala::pcb_rawsbx1.T_RCEXAP", None),
                       None,
                       None),
      RetirementReport("impala::pcb_rawsbx1.T_RCADEP",
                       false,
                       DatasetReport("impala::pcb_rawsbx1.T_RCADEP", None),
                       None,
                       None),
      RetirementReport("impala::pcb_rawsbx1.T_RCECIP",
                       false,
                       DatasetReport("impala::pcb_rawsbx1.T_RCECIP", None),
                       None,
                       None),
      RetirementReport("impala::pcb_rawsbx1.T_RCSAEP",
                       false,
                       DatasetReport("impala::pcb_rawsbx1.T_RCSAEP", None),
                       None,
                       None),
      RetirementReport("impala::pcb_rawsbx1.T_RCEAAP",
                       false,
                       DatasetReport("impala::pcb_rawsbx1.T_RCEAAP", None),
                       None,
                       None),
      RetirementReport("impala::pcb_rawsbx1.T_RCEPIP",
                       false,
                       DatasetReport("impala::pcb_rawsbx1.T_RCEPIP", None),
                       None,
                       None),
      RetirementReport("impala::pcb_rawsbx1.T_RCECMP",
                       false,
                       DatasetReport("impala::pcb_rawsbx1.T_RCECMP", None),
                       None,
                       None),
      RetirementReport("impala::pcb_rawsbx1.T_RCEXCP",
                       false,
                       DatasetReport("impala::pcb_rawsbx1.T_RCEXCP", None),
                       None,
                       None),
      RetirementReport("impala::pcb_rawsbx1.T_RCEXBP",
                       false,
                       DatasetReport("impala::pcb_rawsbx1.T_RCEXBP", None),
                       None,
                       None),
      RetirementReport(
        "T_RCEX1P_3_REP.impala::pcb_rawsbx1",
        false,
        null,
        None,
        Some(
          "org.apache.kudu.client.NonRecoverableException: The table does not exist: table_name: \"impala::pcb_rawsbx1.T_RCEX1P_3_REP\"")
      ),
      RetirementReport("impala::pcb_rawsbx1.T_RCEX4P_REP",
                       false,
                       DatasetReport("impala::pcb_rawsbx1.T_RCEX4P_REP", None),
                       None,
                       None),
      RetirementReport("impala::pcb_rawsbx1.T_RCIVLP_REP",
                       false,
                       DatasetReport("impala::pcb_rawsbx1.T_RCIVLP_REP", None),
                       None,
                       None),
      RetirementReport("impala::pcb_rawsbx1.T_RCEXAP_REP",
                       false,
                       DatasetReport("impala::pcb_rawsbx1.T_RCEXAP_REP", None),
                       None,
                       None),
      RetirementReport("impala::pcb_rawsbx1.T_RCADEP_REP",
                       false,
                       DatasetReport("impala::pcb_rawsbx1.T_RCADEP_REP", None),
                       None,
                       None),
      RetirementReport("impala::pcb_rawsbx1.T_RCECIP_REP",
                       false,
                       DatasetReport("impala::pcb_rawsbx1.T_RCECIP_REP", None),
                       None,
                       None),
      RetirementReport("impala::pcb_rawsbx1.T_RCSAEP_REP",
                       false,
                       DatasetReport("impala::pcb_rawsbx1.T_RCSAEP_REP", None),
                       None,
                       None),
      RetirementReport("impala::pcb_rawsbx1.T_RCEAAP_REP",
                       false,
                       DatasetReport("impala::pcb_rawsbx1.T_RCEAAP_REP", None),
                       None,
                       None),
      RetirementReport("impala::pcb_rawsbx1.T_RCEPIP_REP",
                       false,
                       DatasetReport("impala::pcb_rawsbx1.T_RCEPIP_REP", None),
                       None,
                       None),
      RetirementReport("impala::pcb_rawsbx1.T_RCECMP_REP",
                       false,
                       DatasetReport("impala::pcb_rawsbx1.T_RCECMP_REP", None),
                       None,
                       None),
      RetirementReport("impala::pcb_rawsbx1.T_RCEXCP_REP",
                       false,
                       DatasetReport("impala::pcb_rawsbx1.T_RCEXCP_REP", None),
                       None,
                       None),
      RetirementReport("impala::pcb_rawsbx1.T_RCEXBP_REP",
                       false,
                       DatasetReport("impala::pcb_rawsbx1.T_RCEXBP_REP", None),
                       None,
                       None)
    )

    println(Reporting.toYaml(report))
  }

  test("NPE in toYaml") {
    val report = RetirementReport(
      "T_RCEX1P_3_REP.impala::pcb_rawsbx1",
      false,
      null,
      None,
      Some(
        "org.apache.kudu.client.NonRecoverableException: The table does not exist: table_name: \"impala::pcb_rawsbx1.T_RCEX1P_3_REP\"")
    )
    Reporting.toYaml(List(report))
  }
}
