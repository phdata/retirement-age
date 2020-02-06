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

import io.phdata.retirementage.domain._
import io.phdata.retirementage.storage.HdfsStorage
import io.phdata.retirementage.{SparkTestBase, TestObjects}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._
import org.scalatest.FunSuite

class TableFilterTest extends FunSuite with SparkTestBase {
  val table    = DatedTable("table1", "parquet", "date", 1, None, None, None)
  val database = Database("default", Seq(table))

  test("Filter out a date when column is unix time seconds") {

    val table    = DatedTable("table1", "parquet", "date", 1, None, None, None)
    val database = Database("default", Seq(table))
    val schema   = StructType(StructField("date", LongType, false) :: Nil)
    val data     = TestObjects.smallDatasetSeconds

    val df = createDataFrame(TestObjects.smallDatasetSeconds, schema)

    val filter = getDatedFrameFilter(database, table, df)

    assertResult(2)(filter.expiredRecordsCount())
    assertResult(1)(filter.newDatasetCount())
  }

  test("show expired records") {
    val table    = DatedTable("table1", "parquet", "date", 1, None, None, None)
    val database = Database("default", Seq(table))
    val schema   = StructType(StructField("date", LongType, nullable = false) :: Nil)
    val data     = TestObjects.smallDatasetSeconds

    val df = createDataFrame(data, schema)

    val filter = getDatedFrameFilter(database, table, df)

    assertResult(true)(filter.hasExpiredRecords())
  }

  test("get expired records count") {
    val table    = DatedTable("table1", "parquet", "date", 1, None, None, None)
    val database = Database("default", Seq(table))
    val schema   = StructType(StructField("date", LongType, false) :: Nil)
    val data     = TestObjects.smallDatasetSeconds

    val df = createDataFrame(data, schema)

    val filter = getDatedFrameFilter(database, table, df)

    assertResult(2)(filter.expiredRecordsCount())
  }

  test("dry run on children doesn't change datasets") {}

  test("show no expired records") {
    val table    = DatedTable("table1", "parquet", "date", 1000000000, None, None, None)
    val database = Database("default", Seq(table))
    val schema   = StructType(StructField("date", LongType, nullable = false) :: Nil)
    val data     = TestObjects.smallDatasetSeconds

    val df = createDataFrame(data, schema)

    val filter = getDatedFrameFilter(database, table, df)

    assertResult(false)(filter.hasExpiredRecords())
  }

  test("Filter out a date when column is unix time millis") {
    val table    = DatedTable("table1", "parquet", "date", 1, None, None, None)
    val database = Database("default", Seq(table))

    val data   = TestObjects.smallDatasetSeconds
    val schema = StructType(StructField("date", LongType, false) :: Nil)

    val df = createDataFrame(data, schema)

    val filter = getDatedFrameFilter(database, table, df)

    assertResult(2)(filter.expiredRecordsCount())
  }

  test("Filter out child dataset") {
    val parentSchema = StructType(
      StructField("parentKey", LongType, false) :: StructField("date", LongType, false) :: Nil)
    val childSchema = StructType(
      StructField("childKey", LongType, false) :: StructField("col1", StringType, false) :: Nil)

    val parentTableName = writeTestTable(TestObjects.parentDataset, parentSchema)
    val childTableName  = writeTestTable(TestObjects.childDataset, childSchema)

    val child =
      ChildTable(childTableName, "parquet", List(JoinOn("parentKey", "childKey")), None, None)

    val parent =
      DatedTable(parentTableName, "parquet", "date", 1, None, None, Some(List(child)))

    val database = Database("default", Seq(parent))

    val parentFilter = new DatedTableFilter(database, parent) with HdfsStorage

    val reports = parentFilter.doFilter(true)

    val parentReport = reports.filter(_.qualifiedTableName == s"default.$parentTableName").head

    assertResult(true)(reports.forall(_.recordsRemoved))
    assertResult(Some(1))(parentReport.newDataset.get.count)
  }

  test("Don't filter out child dataset with a hold") {
    val parentSchema = StructType(
      StructField("parentKey", LongType, false) :: StructField("date", LongType, false) :: Nil)
    val childSchema = StructType(
      StructField("childKey", LongType, false) :: StructField("col1", StringType, false) :: Nil)

    val parentTableName = writeTestTable(TestObjects.parentDataset, parentSchema)
    val childTableName  = writeTestTable(TestObjects.childDataset, childSchema)

    val child = ChildTable(childTableName,
                           "parquet",
                           List(JoinOn("parentKey", "childKey")),
                           Some(Hold(true, "legal", "legal@client.biz")),
                           None)

    val parent =
      DatedTable(parentTableName, "parquet", "date", 1, None, None, Some(List(child)))

    val database = Database("default", Seq(parent))

    val parentFilter = new DatedTableFilter(database, parent) with HdfsStorage

    val reports = parentFilter.doFilter(true)
    assertResult(false)(reports.forall(_.recordsRemoved))

    val childReport = reports.filter(_.qualifiedTableName == s"default.$childTableName").head
    assertResult(None)(childReport.newDataset)
  }

  test("Undo deletions") {
    val parentSchema = StructType(
      StructField("parentKey", LongType, false) :: StructField("date", LongType, false) :: Nil)
    val childSchema = StructType(
      StructField("childKey", LongType, false) :: StructField("col1", StringType, false) :: Nil)

    val parentTableName = writeTestTable(TestObjects.parentDataset, parentSchema)
    val childTableName  = writeTestTable(TestObjects.childDataset, childSchema)

    val child =
      ChildTable(childTableName, "parquet", List(JoinOn("parentKey", "childKey")), None, None)

    val table =
      DatedTable(parentTableName, "parquet", "date", 1, None, None, Some(List(child)))

    val database = Database("default", Seq(table))

    val parentFilter = new DatedTableFilter(database, table) with HdfsStorage

    parentFilter.doFilter(false, false)
    val reports = parentFilter.undoDeletions()
    reports.foreach(println)
    assertResult(true)(reports.forall(_.recordsRemoved))

    val childReport = reports.filter(_.qualifiedTableName == s"default.$childTableName").head
    assertResult(s"file:/tmp/$childTableName")(childReport.newDataset.get.location)

    val parentReport = reports.filter(_.qualifiedTableName == s"default.$parentTableName").head
    assertResult(s"file:/tmp/$parentTableName")(parentReport.newDataset.get.location)
  }

  def getDatedFrameFilter(database: Database, table: DatedTable, frame: DataFrame) = {
    class TestTableFilter(database: Database, table: DatedTable)
        extends DatedTableFilter(database, table)
        with HdfsStorage {

      override val currentFrame: DataFrame = frame

      override def removeRecords(computeCountsFlag: Boolean,
                                 dryRun: Boolean,
                                 qualifiedTableName: String,
                                 storageType: String,
                                 currentFrame: DataFrame,
                                 filteredFrame: DataFrame): RetirementReport = {

        RetirementReport(
          qualifiedTableName,
          true,
          DatasetReport("", Some(currentDatasetCount)),
          Some(
            DatasetReport(getCurrentDatasetLocation(s"${database.name}.${table.name}"),
                          Some(filteredFrame.count()))),
          None
        )
      }
    }

    new TestTableFilter(database, table)
  }

  test("filter out nested related datasets") {}

  def getRelatedFrameFilter(database: Database,
                            table: ChildTable,
                            frame: DataFrame,
                            parent: TableFilter) = {
    class TestTableFilter(database: Database, table: ChildTable)
        extends ChildTableFilter(database, table, parent)
        with HdfsStorage {

      override val currentFrame: DataFrame = frame

      override def removeRecords(computeCountsFlag: Boolean,
                                 dryRun: Boolean,
                                 qualifiedTableName: String,
                                 storageType: String,
                                 currentFrame: DataFrame,
                                 filteredFrame: DataFrame): RetirementReport = {

        RetirementReport(
          qualifiedTableName,
          true,
          DatasetReport("", Some(currentDatasetCount)),
          Some(
            DatasetReport(getCurrentDatasetLocation(s"${database.name}.${table.name}"),
                          Some(filteredFrame.count()))),
          None
        )
      }
    }

    new TestTableFilter(database, table)
  }
}
