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
}
