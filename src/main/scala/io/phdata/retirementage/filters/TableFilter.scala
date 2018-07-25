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

import com.typesafe.scalalogging.LazyLogging
import io.phdata.retirementage.SparkDriver.spark
import io.phdata.retirementage.domain._
import io.phdata.retirementage.storage.{HdfsStorage, KuduStorage, StorageActions}
import org.apache.spark.sql.DataFrame

/**
  * Parent abstract class for a dataset filter. A dataset filter can
  *
  * @param database database configuratin domain object
  * @param table    table configuration domain object
  */
abstract class TableFilter(database: Database, table: Table)
    extends StorageActions
    with LazyLogging {
  lazy val currentFrame  = spark.read.table(qualifiedTableName).cache()
  val qualifiedTableName = s"${database.name}.${table.name}"

  /**
    * Count of the dataset after records have been removed
    */
  def newDatasetCount(): Long = currentDatasetCount() - expiredRecordsCount()

  /**
    * Count of dataset as-is
    */
  def currentDatasetCount(): Long = currentFrame.count()

  /**
    * Count of the records that will be removed
    */
  def expiredRecordsCount(): Long = expiredRecords.count()

  def undoDeletions(): Seq[RetirementReport] = {
    if (table.hold.map(_.active).getOrElse(false)) {
      noAction()
    } else {
      val result = try {
        undo(qualifiedTableName)
      } catch {

        case e: Exception =>
          logger.warn(s"Error undoing table deletions $qualifiedTableName", e)

          RetirementReport(
            qualifiedTableName,
            false,
            DatasetReport(getCurrentDatasetLocation(qualifiedTableName), None),
            None,
            Some(e.getMessage)
          )
      }

      val childrenResults = table.child_tables.toSeq.flatten.flatMap { child =>
        val childFilter = new ChildTableFilter(database, child, this) with HdfsStorage

        childFilter.undoDeletions()
      }

      Seq(result) ++ childrenResults
    }
  }

  /**
    * DataFrame with expired records removed
    *
    * @return
    */
  def filteredFrame(): DataFrame

  /**
    * Filter out this table and all children tables
    *
    * @param computeCountsFlag
    * @return
    */
  def doFilter(computeCountsFlag: Boolean, dryRun: Boolean = false): Seq[RetirementReport] = {
    if (table.hold.map(_.active).getOrElse(false)) {
      noAction()
    } else {

      val childrenResults = table.child_tables.toSeq.flatten.flatMap { child =>
        val childFilter = child.storage_type match {
          case "parquet" => new ChildTableFilter(database, child, this) with HdfsStorage
          case "avro" => new ChildTableFilter(database, child, this) with HdfsStorage
          case "kudu" => new ChildTableFilter(database, child, this) with KuduStorage
          case _ => throw new NotImplementedError()
        }

        childFilter.doFilter(computeCountsFlag, dryRun)

      }

      val thisResult = persistFrame(computeCountsFlag,
                                    dryRun,
                                    qualifiedTableName,
                                    table.storage_type,
                                    currentFrame,
                                    filteredFrame())

      Seq(thisResult) ++ childrenResults
    }
  }

  /**
    * Take no action, remove no records
    *
    * @return A report of the nothing that was done
    */
  def noAction(): Seq[RetirementReport] = {
    Seq(
      RetirementReport(qualifiedTableName,
                       false,
                       DatasetReport(getCurrentDatasetLocation(qualifiedTableName)),
                       None,
                       None))
  }

  /**
    * Check whether the table has expired records
    *
    * @return Boolean
    */
  def hasExpiredRecords(): Boolean

  /**
    * Get a <code>DataFrame</code> of all expired records
    *
    * @return <code>DataFrame</code> of expired records
    */
  def expiredRecords(): DataFrame
}
