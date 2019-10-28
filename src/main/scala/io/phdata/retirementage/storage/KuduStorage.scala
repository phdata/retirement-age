/* Copyright 2018 phData Inc. */

package io.phdata.retirementage.storage

import com.typesafe.scalalogging.LazyLogging
import io.phdata.retirementage.SparkDriver.spark
import io.phdata.retirementage.domain.{DatasetReport, GlobalConfig, RetirementReport}
import org.apache.kudu.spark.kudu._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

import scala.collection.JavaConversions._

trait KuduStorage extends StorageActions with LazyLogging {

  override def getCurrentFrame(tableName: String): DataFrame = {
    // Add correct exception to be thrown
    val kuduMasters = GlobalConfig.kuduMasters
      .getOrElse(throw new IllegalArgumentException("kuduMasters were not found"))
      .mkString(",")
    spark.sqlContext.read
      .options(Map("kudu.master" -> kuduMasters, "kudu.table" -> tableName))
      .kudu
  }

  override def removeRecords(computeCountsFlag: Boolean,
                             dryRun: Boolean,
                             qualifiedTableName: String,
                             storageType: String,
                             currentFrame: DataFrame,
                             filteredFrame: DataFrame): RetirementReport = {
    try {
      val kuduMasters = GlobalConfig.kuduMasters
        .getOrElse(throw new IllegalArgumentException("kuduMasters were not found"))
        .mkString(",")
      val kuduContext =
        new KuduContext(kuduMasters, spark.sqlContext.sparkContext)

      val currentDatasetCount = if (computeCountsFlag) Some(currentFrame.count()) else None

      val newDatasetCount =
        if (computeCountsFlag) Some(currentFrame.count() - filteredFrame.count()) else None

      if (!dryRun) {
        logger.info(s"deleting expired rows in $qualifiedTableName")
        val primaryKeys = kuduContext.syncClient
          .openTable(qualifiedTableName)
          .getSchema
          .getPrimaryKeyColumns
          .map(k => col(k.getName))

        // Selecting keys to delete from the kudu table
        val filteredKeys = filteredFrame.select(primaryKeys: _*)
        kuduContext.deleteRows(filteredKeys, qualifiedTableName)

        RetirementReport(qualifiedTableName,
                         true,
                         DatasetReport(qualifiedTableName, currentDatasetCount),
                         Some(DatasetReport(qualifiedTableName, newDatasetCount)),
                         None)
      }
      RetirementReport(
        qualifiedTableName,
        true,
        DatasetReport(qualifiedTableName, currentDatasetCount),
        Some(DatasetReport(qualifiedTableName, newDatasetCount)),
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

  override def getNewDatasetLocation(qualifiedTableName: String): String = {
    // No new dataset location for kudu tables
    qualifiedTableName
  }

  override def getCurrentDatasetLocation(qualifiedTableName: String): String = {
    qualifiedTableName
  }

  override def undo(qualifiedTableName: String): RetirementReport =
    throw new NotImplementedError("Cannot undo Kudu table deletes")
}
