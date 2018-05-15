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

package io.phdata.retirementage.filters

import io.phdata.retirementage.domain.{Database, DatedTable}
import io.phdata.retirementage.storage.HdfsStorage
import io.phdata.retirementage.{SparkTestBase, TestObjects}
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.types.{LongType, StructField, StructType}
import org.scalatest.FunSuite

class HdfsStorageTest extends FunSuite with SparkTestBase {

  private val catalogInfo =
    """CatalogTable(
      |       Table: `default`.`table1`
      |       Owner: owner@domain.com
      |       Created: Wed Dec 27 15:57:17 UTC 2017
      |       Last Access: Thu Jan 01 00:00:00 UTC 1970
      |       Type: MANAGED
      |       Schema: [StructField(eventid,LongType,true), StructField(eventdate,TimestampType,true)]
      |       Provider: hive
      |       Properties: [numFiles=4, transient_lastDdlTime=1514391377, SOURCE=Oracle GIRD, LOAD_FREQUENCY=ONCE, totalSize=14133221, SECURITY_CLASSIFICATION=confidential, CONTACT_INFO=user@company.com, COLUMN_STATS_ACCURATE=true, numRows=91535]
      |       Storage(Location: hdfs://ns1/data/table1, InputFormat: org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat, OutputFormat: org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat, Serde: org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe)
      |       Partition Provider: Catalog)""".stripMargin

  // @TODO move to TableFilterTest
  test("filter various data types date fields") {
    val table    = DatedTable("table1", "parquet", "date", 1, None, None, None)
    val database = Database("default", Seq(table))

    // This snappy/parquet file contains many different timestamp formats taken from an Impala table
    val df = spark.read.parquet("test/timestamps.snappy.parquet")
    df.write.mode(SaveMode.Overwrite).saveAsTable("default.table1")

    val filter1 = new DatedTableFilter(database,
                                       table.copy(expiration_column = "string_type_dateonly",
                                                  date_format_string = Some("yyyy-MM-dd")))
    with HdfsStorage

    assertResult(0)(filter1.newDatasetCount())

    val filter2 =
      new DatedTableFilter(database, table.copy(expiration_column = "timestamp_type_dateonly"))
      with HdfsStorage

    assertResult(0)(filter2.newDatasetCount())

    val filter3 =
      new DatedTableFilter(database, table.copy(expiration_column = "epoch_bigint_type_dateonly"))
      with HdfsStorage

    assertResult(0)(filter3.newDatasetCount())

    val filter4 =
      new DatedTableFilter(database,
                           table.copy(expiration_column = "string_type_local_dateandtime",
                                      date_format_string = Some("yy-MM-dd HH:mm:ss.SSS")))
      with HdfsStorage

    assertResult(0)(filter4.newDatasetCount())

    val filter5 =
      new DatedTableFilter(database,
                           table.copy(expiration_column = "timestamp_type_local_dateandtime"))
      with HdfsStorage

    assertResult(0)(filter5.newDatasetCount())

    val filter6 =
      new DatedTableFilter(database,
                           table.copy(expiration_column = "string_type_utc_dateandtime",
                                      date_format_string = Some("yy-MM-dd HH:mm:ss.SSS")))
      with HdfsStorage

    assertResult(0)(filter6.newDatasetCount())

    val filter7 =
      new DatedTableFilter(database,
                           table.copy(expiration_column = "timestamp_type_utc_dateandtime"))
      with HdfsStorage

    assertResult(0)(filter7.newDatasetCount())
  }

  test("write new dataset") {
    val table    = DatedTable("table2", "parquet", "date", 1, None, None, None)
    val database = Database("default", Seq(table))
    val schema   = StructType(StructField("date", LongType, false) :: Nil)
    val data     = TestObjects.smallDatasetSeconds

    val df = createDataFrame(data, schema)
    df.write.mode(SaveMode.Overwrite).saveAsTable(s"${database.name}.${table.name}")

    val filter = new DatedTableFilter(database, table) with HdfsStorage

    val result = filter.doFilter(true)

    val newDatasetCount = spark.read.parquet(result(0).newDataset.get.location).count()
    assertResult(1)(filter.newDatasetCount())

    assertResult(1)(newDatasetCount)
  }

  test("don't make changes on a dry run") {
    val table    = DatedTable("table2", "parquet", "date", 1, None, None, None)
    val database = Database("default", Seq(table))
    val schema   = StructType(StructField("date", LongType, false) :: Nil)
    val data     = TestObjects.smallDatasetSeconds

    val df = createDataFrame(data, schema)
    df.write.mode(SaveMode.Overwrite).saveAsTable(s"${database.name}.${table.name}")

    val filter = new DatedTableFilter(database, table) with HdfsStorage

    val result = filter.doFilter(computeCountsFlag = true, dryRun = true)
    assertThrows[Exception](spark.read.parquet(result(0).newDataset.get.location).count())
  }

  test("get current dataset location") {
    val table    = DatedTable("table1", "parquet", "date", 1, None, None, None)
    val database = Database("default", Seq(table))
    val schema   = StructType(StructField("date", LongType, false) :: Nil)
    val data     = TestObjects.smallDatasetSeconds

    val df = createDataFrame(data, schema)
    df.write.mode(SaveMode.Overwrite).saveAsTable(s"${database.name}.${table.name}")

    val filter: TableFilter = new DatedTableFilter(database, table) with HdfsStorage

    // initial location
    assertResult("file:/tmp/table1")(filter.getCurrentDatasetLocation(filter.qualifiedTableName))

  }

  test("get new dataset location") {
    val table    = DatedTable("table1", "parquet", "date", 1, None, None, None)
    val database = Database("default", Seq(table))
    val schema   = StructType(StructField("date", LongType, false) :: Nil)
    val data     = TestObjects.smallDatasetSeconds

    val df = createDataFrame(data, schema)
    df.write.mode(SaveMode.Overwrite).saveAsTable(s"${database.name}.${table.name}")

    val filter: TableFilter = new DatedTableFilter(database, table) with HdfsStorage

    // initial location
    assertResult("file:/tmp/table1_ra")(filter.getNewDatasetLocation(filter.qualifiedTableName))

  }

  test("parse location from detailed table information") {
    val locationRegex = "Storage\\(Location:[^,]*".r

    assertResult("hdfs://ns1/data/table1")(
      locationRegex.findFirstIn(catalogInfo).get.split("Location:")(1).trim)
  }

  test("parse numFiles from detailed table information") {
    val propertiesRegex = "Properties: \\[(.*)\\]".r("properties")

    assertResult("4")(
      propertiesRegex
        .findFirstIn(catalogInfo)
        .map(
          x =>
            x.split(", ")
              .filter(_.contains("numFiles"))
              .head
              .split("=")(1))
        .getOrElse(throw new Exception("properties not found")))
  }

}
