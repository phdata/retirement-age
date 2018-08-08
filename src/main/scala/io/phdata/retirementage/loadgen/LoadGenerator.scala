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

package io.phdata.retirementage.loadgen

import java.util.UUID

import io.phdata.retirementage.domain.GlobalConfig
import org.apache.kudu.client.CreateTableOptions
import org.apache.kudu.spark.kudu.KuduContext
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql._
import org.apache.spark.sql.functions.lit
import org.apache.spark.storage.StorageLevel

import scala.collection.JavaConverters._

object LoadGenerator {
  def main(args: Array[String]): Unit = {

    val conf = new LoadGeneratorConfig(args)

    val spark = SparkSession
      .builder()
      .enableHiveSupport()
      .appName("load-generator")
      .config("spark.sql.parquet.compression.codec", "snappy")
      .config("spark.sql.parquet.binaryAsString", "true")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()

    if (conf.outputFormat() == "kudu") {
      val mastersList = conf.kuduMasters().split(",").toList

      GlobalConfig.kuduMasters = Some(mastersList)
    }

    generateTables(spark, conf)
  }

  def generateTables(spark: SparkSession, conf: LoadGeneratorConfig): Unit = {
    // generate fact table
    val factDf = generateTable(spark, conf.factCount(), 1).persist(StorageLevel.MEMORY_AND_DISK)

    // generate dimension table
    val dimensionDf =
      generateTableFromParent(spark, conf.dimensionCount(), 2, conf.factCount(), factDf)
        .persist(StorageLevel.MEMORY_AND_DISK)

    // generate subdimension table
    val subDimensionDf = generateTableFromParent(spark,
                                                 conf.subDimensionCount(),
                                                 3,
                                                 conf.dimensionCount(),
                                                 dimensionDf).persist(StorageLevel.MEMORY_AND_DISK)

    conf.outputFormat() match {
      case "parquet" =>
        factDf
          .coalesce(50)
          .write
          .mode(SaveMode.Overwrite)
          .saveAsTable(s"${conf.databaseName()}.${conf.factName()}")
        dimensionDf
          .repartition(25)
          .write
          .mode(SaveMode.Overwrite)
          .saveAsTable(s"${conf.databaseName()}.${conf.dimName()}")
        subDimensionDf
          .repartition(25)
          .write
          .mode(SaveMode.Overwrite)
          .saveAsTable(s"${conf.databaseName()}.${conf.subName()}")
      case "kudu" =>
        val kuduContext =
          new KuduContext(GlobalConfig.kuduMasters.get.mkString(","), spark.sqlContext.sparkContext)
        
        createKuduTable(kuduContext, conf.factName(), 10)
        kuduContext.insertRows(factDf, conf.factName())

        createKuduTable(kuduContext, conf.dimName(), 10)
        kuduContext.insertRows(dimensionDf, conf.dimName())

        createKuduTable(kuduContext, conf.subName(), 10)
        kuduContext.insertRows(subDimensionDf, conf.subName())
      case _ => throw new UnsupportedOperationException
    }
  }

  case class LoadGenTemp(id: String, payload: String, dimension_id: String, expiration_date: String)

  /**
    * Generates a test dataframe
    * @param numRecords Number of records in the dataframe
    * @param payloadBytes Payload size
    * @return The test dataframe
    */
  def generateTable(spark: SparkSession, numRecords: Int, payloadBytes: Int) = {

    /**
      * Create the following schema:
      *   -- id: String (nullable = false)
      *   -- payload: String (nullable = false)
      *   -- dimension_id: String (nullable = false)
      *   -- expirationDate: String (nullabe = false)
      */
    val schema = StructType(StructField("id", StringType, false) :: Nil)
      .add(StructField("payload", StringType, false))
      .add(StructField("dimension_id", StringType, false))
      .add(StructField("expiration_date", StringType, false))
    // Create a string with a number of bytes
    val byteString = "a" * payloadBytes

    // Maximum number of records per split DataFrame
    val maximumDfRecords = 100000

    // Calculating the number of split DataFrames to create and the records per split DataFrame
    val numDataFrames = math.ceil(numRecords.toDouble / maximumDfRecords).toInt
    val recordsPerDf  = math.ceil(numRecords.toDouble / numDataFrames).toInt

    //Creating DataFrame with duplicated data
    val newdata = Range(0, recordsPerDf).map(x => Seq("a", "b", "c", "d"))
    val rows    = newdata.map(x => Row(x: _*))
    val rdd     = spark.sparkContext.makeRDD(rows)
    val df      = spark.createDataFrame(rdd, schema)

    val dfs = Range(0, numDataFrames).map(x => df)

    // Union the Sequence of DataFrames onto finalTable
    val duplicateDf: DataFrame = dfs.reduce((l, r) => l.union(r))

    import spark.implicits._
    // Create temporary dataset to make changes on
    val tempDs: Dataset[LoadGenTemp] = duplicateDf.as[LoadGenTemp]

    // Create final DataSet with all random data
    val finalDs: Dataset[LoadGenTemp] = tempDs.map {
      case change =>
        change
          .copy(id = UUID.randomUUID().toString,
                dimension_id = UUID.randomUUID().toString,
                expiration_date = tempDateGenerator())
    }

    finalDs.toDF()
  }

  // Create date data with 2016-12-25, 2017-12-25, 2018-12-25
  def tempDateGenerator(): String = {
    val r = new scala.util.Random()
    val c = 0 + r.nextInt(3)
    c match {
      case 0 => "2016-12-25"
      case 1 => "2017-12-25"
      case 2 => "2018-12-25"
      case _ => "2222-22-22"
    }
  }

  /**
    * Generates a test dataframe with keys from the parent used as foreign keys
    * @param numRecords Number of records in the dataframe
    * @param payloadBytes Payload size
    * @return The test dataframe
    */
  def generateTableFromParent(spark: SparkSession,
                              numRecords: Int,
                              payloadBytes: Int,
                              parentNumRecords: Int,
                              parent: DataFrame): DataFrame = {

    val byteString = "a" * payloadBytes

    // Create a temporary dimension dataframe with the incorrect column names
    val oldDimensionDf = parent
      .select("dimension_id")
      .limit(numRecords)
      .withColumn("payload", lit(byteString))
      .withColumn("subdimension_id", lit(UUID.randomUUID().toString()))
      .withColumn("expiration_date", lit("2222-22-22"))
    // Correct column names
    val newNames = Seq("id", "payload", "dimension_id", "expiration_date")
    // Create a dimension DF with correct column names
    val correctSchemaDf = oldDimensionDf.toDF(newNames: _*)

    // Create random dimension_id UUID's
    import spark.implicits._
    // Create temporary dataset to make changes on
    val tempDs: Dataset[LoadGenTemp] = correctSchemaDf.as[LoadGenTemp]
    // Create final DataSet with random dimension_ids
    val finalDs: Dataset[LoadGenTemp] = tempDs.map {
      case change =>
        change.copy(dimension_id = UUID.randomUUID().toString)
    }
    val finalDf = finalDs.toDF()

    // If numRecords > parentNumRecords creates the difference to union to the dimension dataframe
    if (numRecords > parentNumRecords) {
      val newRowsNum = numRecords - parentNumRecords
      val tempDf     = generateTable(spark, newRowsNum, payloadBytes)

      finalDf.union(tempDf)
    } else {
      finalDf
    }

  }

  /**
    * Generates a kudu table
    * @param kuduContext
    * @param tableName: Name of kudu table to create
    * @param numBuckets: Number of buckets in kudu table
    */
  def createKuduTable(kuduContext: KuduContext, tableName: String, numBuckets: Int): Unit = {
    val defaultSchema = StructType(StructField("id", StringType, false) :: Nil)
      .add(StructField("payload", StringType, false))
      .add(StructField("dimension_id", StringType, false))
      .add(StructField("expiration_date", StringType, false))
    val primaryKey = Seq("id")

    val tableOptions = new CreateTableOptions()
      .setNumReplicas(3)
      .addHashPartitions(List(primaryKey(0)).asJava, numBuckets)

    if (kuduContext.tableExists(tableName)) {
      kuduContext.deleteTable(tableName)
    }
    kuduContext.createTable(tableName, defaultSchema, primaryKey, tableOptions)
  }
}
