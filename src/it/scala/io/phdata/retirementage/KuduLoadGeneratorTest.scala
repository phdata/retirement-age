package io.phdata.retirementage

import io.phdata.retirementage.domain.GlobalConfig
import io.phdata.retirementage.loadgen.{LoadGenerator, LoadGeneratorConfig}
import org.apache.kudu.spark.kudu._
import org.apache.spark.sql.SparkSession
import org.scalatest.FunSuite

class KuduLoadGeneratorTest extends FunSuite with SparkTestBase {
  GlobalConfig.kuduMasters = Some(List("localhost:7051"))
  val kuduMaster = GlobalConfig.kuduMasters.get.mkString(",")
  val kuduContext =
    new KuduContext(kuduMaster, spark.sqlContext.sparkContext)

  def confSetup(factNum: Int, dimNum: Int, subNum: Int): LoadGeneratorConfig = {
    val confArgs = Array("--fact-count",
                         factNum.toString,
                         "--dimension-count",
                         dimNum.toString,
                         "--subdimension-count",
                         subNum.toString,
                         "--output-format",
                         "kudu")

    new LoadGeneratorConfig(confArgs)
  }

  test("Create kudu table with specified payload and record size") {
    // Setup
    val conf = confSetup(10000, 0, 0)

    // When
    LoadGenerator.generateTables(spark: SparkSession, conf: LoadGeneratorConfig)

    // Then
    val actual =
      spark.sqlContext.read
        .options(Map("kudu.master" -> kuduMaster, "kudu.table" -> conf.factName()))
        .kudu

    assertResult(10000)(actual.count())
  }

  test("Create dimensional kudu table where dimensional-count < fact-count") {
    // Setup
    val conf = confSetup(2000, 1000, 0)

    // When
    LoadGenerator.generateTables(spark: SparkSession, conf: LoadGeneratorConfig)

    // Then
    val actual =
      spark.sqlContext.read
        .options(Map("kudu.master" -> kuduMaster, "kudu.table" -> conf.dimName()))
        .kudu

    assertResult(1000)(actual.count())
  }

  test("Create dimensional kudu table where dimensional-count > fact-count") {
    // Setup
    val conf = confSetup(1000, 2000, 0)

    // When
    LoadGenerator.generateTables(spark: SparkSession, conf: LoadGeneratorConfig)

    // Then
    val actual =
      spark.sqlContext.read
        .options(Map("kudu.master" -> kuduMaster, "kudu.table" -> conf.dimName()))
        .kudu

    assertResult(2000)(actual.count())
  }

  test("Create subdimensional kudu table where subdimensional-count < dimensional-count") {
    // Setup
    val conf = confSetup(2000, 1000, 500)

    // When
    LoadGenerator.generateTables(spark: SparkSession, conf: LoadGeneratorConfig)

    // Then
    val actual =
      spark.sqlContext.read
        .options(Map("kudu.master" -> kuduMaster, "kudu.table" -> conf.subName()))
        .kudu

    assertResult(500)(actual.count())
  }

  test("Create subdimensional kudu table where subdimensional-count > dimensional-count") {
    // Setup
    val conf = confSetup(2000, 1000, 2000)

    // When
    LoadGenerator.generateTables(spark: SparkSession, conf: LoadGeneratorConfig)

    // Then
    val actual =
      spark.sqlContext.read
        .options(Map("kudu.master" -> kuduMaster, "kudu.table" -> conf.subName()))
        .kudu

    assertResult(2000)(actual.count())
  }
}
