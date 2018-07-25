/* Copyright 2018 phData Inc. */

package io.phdata.retirementage.storage
import io.phdata.retirementage.domain.RetirementReport
import org.apache.spark.sql.DataFrame

trait KuduStorage extends StorageActions {
  override def persistFrame(computeCountsFlag: Boolean, dryRun: Boolean, qualifiedTableName: String, storageType: String, currentFrame: DataFrame, filteredFrame: DataFrame): RetirementReport = ???

  override def getNewDatasetLocation(qualifiedTableName: String): String = ???

  override def getCurrentDatasetLocation(qualifiedTableName: String): String = ???

  override def undo(qualifiedTableName: String): RetirementReport = throw new NotImplementedError("Cannot undo Kudu table deletes")
}
