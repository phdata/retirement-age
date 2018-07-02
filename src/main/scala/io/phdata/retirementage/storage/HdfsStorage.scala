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

package io.phdata.retirementage.storage

import com.typesafe.scalalogging.LazyLogging
import io.phdata.retirementage.SparkDriver.spark
import io.phdata.retirementage.domain._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

/**
  * Handles removing data from HDFS
  */
trait HdfsStorage extends StorageActions with LazyLogging {

  override def persistFrame(computeCountsFlag: Boolean,
                            dryRun: Boolean,
                            qualifiedTableName: String,
                            storageType: String,
                            currentFrame: DataFrame,
                            filteredFrame: DataFrame) = {
    try {
      val originalDatasetLocation = getCurrentDatasetLocation(qualifiedTableName)
      val newDatasetLocation      = getNewDatasetLocation(qualifiedTableName)

      val currentDatasetCount = if (computeCountsFlag) Some(currentFrame.count()) else None

      val newDatasetCount =
        if (computeCountsFlag) Some(filteredFrame.count()) else None

      if (!dryRun) {
        logger.info(s"writing table $qualifiedTableName to path $newDatasetLocation")

        // coalesce to the original number of files
        val coalescedFrame = filteredFrame.coalesce(getNumFiles(qualifiedTableName))

        storageType match {
          case "parquet" =>
            coalescedFrame.write.mode(SaveMode.Overwrite).parquet(newDatasetLocation)
          case _ =>
            RetirementReport(
              qualifiedTableName,
              true,
              DatasetReport(originalDatasetLocation, currentDatasetCount),
              None,
              Some("no data filteredFrame writer configured for type: " + storageType)
            )
        }

        logger.info(s"altering table location for $qualifiedTableName")
        alterLocation(qualifiedTableName)
      }

      RetirementReport(
        qualifiedTableName,
        true,
        DatasetReport(originalDatasetLocation, currentDatasetCount),
        Some(DatasetReport(newDatasetLocation, newDatasetCount)),
        None
      )
    } catch {
      case e: Exception => {
        logger.error(s"exception writing $qualifiedTableName", e)
        RetirementReport(qualifiedTableName,
                         false,
                         DatasetReport(getCurrentDatasetLocation(qualifiedTableName)),
                         None,
                         Some(e.getMessage))
      }
    }
  }

  def alterLocation(qualifiedTableName: String) = {

    val originalDatasetLocation = getCurrentDatasetLocation(qualifiedTableName)
    val newDatasetLocation      = getNewDatasetLocation(qualifiedTableName)

    spark.sql(s"alter table $qualifiedTableName set location '$newDatasetLocation'")

    // Refresh HiveContext metadata for testing/assertion purposes. This will not refresh
    // Impala metadata
    spark.sql(s"refresh table $qualifiedTableName")

  }

  def getNewDatasetLocation(qualifiedTableName: String) = {
    val retirementAgeSuffix = "_ra"

    if (getCurrentDatasetLocation(qualifiedTableName).endsWith(retirementAgeSuffix)) {
      getCurrentDatasetLocation(qualifiedTableName).dropRight(3)
    } else {
      getCurrentDatasetLocation(qualifiedTableName) + retirementAgeSuffix
    }
  }

  def getCurrentDatasetLocation(qualifiedTableName: String) = {
    // spark2.2+ dataframe schema
    val details: DataFrame = spark.sql(s"describe extended $qualifiedTableName")

    details.filter(col("col_name") === "Location").select("data_type").collect().head.getString(0)

  }

  def getNumFiles(qualifiedTableName: String): Int =
    spark.sparkContext.wholeTextFiles(getCurrentDatasetLocation(qualifiedTableName)).count().toInt

  override def undo(qualifiedTableName: String): RetirementReport = {
    val originalDatasetLocation = getCurrentDatasetLocation(qualifiedTableName)
    val newDatasetLocation      = getNewDatasetLocation(qualifiedTableName)

    alterLocation(qualifiedTableName)

    RetirementReport(
      qualifiedTableName,
      true,
      DatasetReport(originalDatasetLocation, None),
      Some(DatasetReport(newDatasetLocation, None)),
      None
    )
  }

}
