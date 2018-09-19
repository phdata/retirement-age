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

import com.amazonaws.services.kinesis.model.InvalidArgumentException
import com.typesafe.scalalogging.LazyLogging
import io.phdata.retirementage.domain._
import io.phdata.retirementage.filters._
import io.phdata.retirementage.storage.{HdfsStorage, KuduStorage}
import org.apache.spark.sql.SparkSession

object SparkDriver extends LazyLogging {

  val spark = SparkSession
    .builder()
    .appName("retirement-age")
    .config("spark.sql.parquet.compression.codec", "snappy")
    .config("spark.sql.parquet.binaryAsString", "true")
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .enableHiveSupport()
    .getOrCreate()

  def retire(config: Config,
             computeCountsFlag: Boolean,
             dryRunFlag: Boolean,
             undoFlag: Boolean): Seq[RetirementReport] = {

    config.databases.flatMap { database =>
      database.tables.flatMap { table =>
        val hold                = table.hold.map(_.active).getOrElse(false)
        val filter: TableFilter = getFilter(database, table)

        try {
          if (undoFlag) {
            filter.undoDeletions()
          } else if (dryRunFlag) {
            filter.doFilter(computeCountsFlag, dryRun = true)
          } else if (hold || !filter.hasExpiredRecords()) {
            filter.noAction()
          } else {
            // table has no hold and needs to be filtered, write a new dataset without expired records
            filter.doFilter(computeCountsFlag, dryRun = false)
          }
        } catch {
          case e: Exception =>
            val fullTableName = s"${table.name}.${database.name}"
            logger.error(s"Error processing $fullTableName", e)
            Seq(RetirementReport(fullTableName, false, null, None, Some(s"${e.toString}")))
        }
      }
    }
  }

  def getFilter(database: Database, table: Table) = {
    table match {
      case d: DatedTable =>
        d.storage_type match {
          case "parquet" =>
            new DatedTableFilter(database, d) with HdfsStorage
          case "avro" =>
            new DatedTableFilter(database, d) with HdfsStorage
          case "kudu" =>
            new DatedTableFilter(database, d) with KuduStorage
          case _ => throw new NotImplementedError()
        }
      case c: CustomTable =>
        c.storage_type match {
          case "parquet" =>
            new CustomTableFilter(database, c) with HdfsStorage
          case "avro" =>
            new CustomTableFilter(database, c) with HdfsStorage
          case "kudu" =>
            new CustomTableFilter(database, c) with KuduStorage
          case _ => throw new NotImplementedError()
        }

    }
  }
}
