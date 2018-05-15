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

import io.phdata.retirementage.domain.{Config, Database, DatedTable, Hold}
import io.phdata.retirementage.filters.DatedTableFilter
import io.phdata.retirementage.storage.HdfsStorage
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.types._
import org.scalatest.FunSuite

class SparkDriverTest extends FunSuite with SparkTestBase {

  test("filtering table reduces record count") {
    val table    = DatedTable("table1", "parquet", "date", 1, None, None, None)
    val database = Database("default", Seq(table))
    val config =
      Config(None, List(database))
    val schema = StructType(StructField(table.expiration_column, LongType, false) :: Nil)

    val data = TestObjects.smallDatasetSeconds

    val df = createDataFrame(data, schema)
    df.write.mode(SaveMode.Overwrite).saveAsTable(s"${database.name}.${table.name}")

    val filter      = new DatedTableFilter(database, table) with HdfsStorage
    val beforeCount = spark.read.table(s"${database.name}.${table.name}").count()

    SparkDriver.retire(config, computeCountsFlag = false, dryRunFlag = false, undoFlag = false)

    val afterCount = spark.read.table(s"${database.name}.${table.name}").count()
    assert(beforeCount > afterCount)
  }

  test("don't retire table with hold") {
    val table = DatedTable("table1",
                           "parquet",
                           "date",
                           1,
                           Some(Hold(true, "legal", "tony@phdata.io")),
                           None,
                           None)
    val database = Database("default", Seq(table))
    val config =
      Config(None, List(database))
    val schema = StructType(StructField(table.expiration_column, LongType, false) :: Nil)

    val data = TestObjects.smallDatasetSeconds

    val df = createDataFrame(data, schema)
    df.write.mode(SaveMode.Overwrite).saveAsTable(s"${database.name}.${table.name}")

    val filter = new DatedTableFilter(database, table) with HdfsStorage

    val beforeCount = spark.read.table(s"${database.name}.${table.name}").count()

    SparkDriver.retire(config, false, false, false)

    val afterCount = spark.read.table(s"${database.name}.${table.name}").count()

    assert(beforeCount == afterCount)
  }

  test("dry run doesn't change any records") {
    val table    = DatedTable("table1", "parquet", "date", 1, None, None, None)
    val database = Database("default", Seq(table))
    val config =
      Config(None, List(database))
    val schema = StructType(StructField(table.expiration_column, LongType, false) :: Nil)

    val data = TestObjects.smallDatasetSeconds

    val df = createDataFrame(data, schema)
    df.write.mode(SaveMode.Overwrite).saveAsTable(s"${database.name}.${table.name}")
    val filter = new DatedTableFilter(database, table) with HdfsStorage

    val beforeCount = spark.read.table(s"${database.name}.${table.name}").count()

    SparkDriver.retire(config, false, true, false)

    val afterCount = spark.read.table(s"${database.name}.${table.name}").count()

    assert(beforeCount == afterCount)
  }

}
