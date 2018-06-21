/* Copyright 2018 phData Inc. */

package io.phdata.retirementage.loadgen

import org.apache.spark.sql.{DataFrame, SparkSession}

object LoadGenerator {
  def main(args: Array[String]): Unit = {

    val conf  = new LoadGeneratorConfig(args)
    val spark = new SparkSession()
    generateTables(spark, conf)
  }

  def generateTables(spark: SparkSession, conf: LoadGeneratorConfig): Unit = {
    // generate fact table
    // write fact table to disk
    // generate dimension table
    // write dimension table to disk
    // generate subdimension table
    // write subdimension table to disk
  }

  /**
    * Generates a test dataframe
    * @param numRecords Number of records in the dataframe
    * @param payloadBytes Payload size
    * @return The test dataframe
    */
  def generateTable(numRecords: Int, payloadBytes: Int): DataFrame = ???

  /**
    * Generates a test dataframe with keys from the parent used as foreign keys
    * @param numRecords Number of records in the dataframe
    * @param payloadBytes Payload size
    * @return The test dataframe
    */
  def generateTableFromParent(numRecords: Int, payloadBytes: Int, parent: DataFrame): DataFrame = ???
}
