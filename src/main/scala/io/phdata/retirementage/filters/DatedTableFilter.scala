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
import io.phdata.retirementage.storage.{HdfsStorage, StorageActions}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Column, DataFrame}

import scala.util.{Failure, Success, Try}

/**
  * Filter for a dataset containing a date column
  * @inheritdoc
  */
abstract class DatedTableFilter(database: Database, table: DatedTable)
    extends TableFilter(database, table) {
  lazy val dateExpression: Column =
    getDateExpression(currentFrame, table.expiration_column, table.date_format_string)

  val DEFAULT_STRING_FORMAT = "yyyy-MM-dd"

  /**
    * @inheritdoc
    */
  override def expiredRecords(): DataFrame =
    currentFrame
      .filter(date_add(dateExpression, table.expiration_days) < current_date())
      .cache()

  override def filteredFrame() =
    currentFrame
      .filter(date_add(dateExpression, table.expiration_days) > current_date())

  /**
    * @inheritdoc
    */
  override def hasExpiredRecords(): Boolean = {

    val minDate = currentFrame.agg(min(dateExpression).as(table.expiration_column))

    val minDateFrame =
      minDate.filter(date_add(col(table.expiration_column), table.expiration_days) < current_date())

    !minDateFrame.rdd
      .isEmpty()
  }

  /**
    * Get a date expression based on the column datatype
    * @param df The dataframe
    * @param dateColumn Select date column
    * @param stringFormat Override date format
    * @return
    */
  def getDateExpression(df: DataFrame, dateColumn: String, stringFormat: Option[String]): Column = {
    val dateColumnType: Try[DataType] = Try(
      df.schema.filter(f => f.name == dateColumn).head.dataType)

    val dateExpression = dateColumnType match {
      case Success(StringType) =>
        from_unixtime(
          unix_timestamp(col(dateColumn), stringFormat.getOrElse(DEFAULT_STRING_FORMAT)))
      case Success(DateType) =>
        col(dateColumn)
      case Success(LongType) =>
        // If it's a long it could be a unix timestamp in seconds or milliseconds
        val firstColumn = df.select(col(dateColumn).cast(StringType)).first().getString(0)
        firstColumn.length() match {
          case 16 => from_unixtime((col(dateColumn) / (1000 * 1000)).cast(LongType)) // microseconds
          case 13 => from_unixtime((col(dateColumn) / 1000).cast(LongType)) // milliseconds
          case 10 => from_unixtime(col(dateColumn)) // seconds
        }
      case Success(TimestampType) =>
        col(dateColumn).cast(DateType)
      case Success(_) => throw new IllegalArgumentException("Date expression not recognized.")
      case Failure(e) => throw e
    }
    dateExpression
  }

}
