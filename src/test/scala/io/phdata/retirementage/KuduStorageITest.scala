package io.phdata.retirementage

import java.util.UUID

import io.phdata.retirementage.domain._
import io.phdata.retirementage.filters.DatedTableFilter
import io.phdata.retirementage.storage.KuduStorage
import org.apache.kudu.client._
import org.apache.kudu.spark.kudu._
import org.apache.kudu.test.KuduTestHarness
import org.apache.spark.sql.types._
import org.scalatest.{BeforeAndAfter, FunSuite}
import org.scalatest.Matchers._

import scala.collection.JavaConverters._

class KuduStorageITest extends FunSuite with SparkTestBase with BeforeAndAfter {

  val harness                  = new KuduTestHarness()
  var kuduMaster: String       = _
  var kuduContext: KuduContext = _

  var subTableName  = ""
  var dimTableName  = ""
  var factTableName = ""

  var subdimTable = ChildTable(subTableName, "kudu", KuduObjects.defaultDimJoin, None, None)
  var dimTable =
    ChildTable(dimTableName, "kudu", KuduObjects.defaultFactJoin, None, Some(List(subdimTable)))
  var factTable = DatedTable("test", "kudu", "date", 1, None, None, Some(List(dimTable)))
  var database  = Database("default", Seq(factTable))

  var subQualifiedTableName  = ""
  var dimQualifiedTableName  = ""
  var factQualifiedTableName = ""

  before {
    harness.before()
    kuduMaster = harness.getMasterAddressesAsString
    GlobalConfig.kuduMasters = Some(harness.getMasterServers.asScala.map(_.toString).toList)
    kuduContext = new KuduContext(kuduMaster, spark.sqlContext.sparkContext)
    factTableName =
      createKuduTable(KuduObjects.defaultFactSchema, KuduObjects.defaultFactKey, kuduContext)
    dimTableName =
      createKuduTable(KuduObjects.defaultDimSchema, KuduObjects.defaultDimKey, kuduContext)
    subTableName =
      createKuduTable(KuduObjects.defaultSubSchema, KuduObjects.defaultSubKey, kuduContext)

    subdimTable = ChildTable(subTableName, "kudu", KuduObjects.defaultDimJoin, None, None)
    dimTable =
      ChildTable(dimTableName, "kudu", KuduObjects.defaultFactJoin, None, Some(List(subdimTable)))
    factTable = DatedTable(factTableName, "kudu", "date", 1, None, None, Some(List(dimTable)))
    database = Database("impala::default", Seq(factTable))

    insertKuduData(factTable, KuduObjects.defaultFactData, KuduObjects.defaultFactSchema)

    insertKuduData(dimTable, KuduObjects.defaultDimData, KuduObjects.defaultDimSchema)

    insertKuduData(subdimTable, KuduObjects.defaultSubData, KuduObjects.defaultSubSchema)

    subQualifiedTableName = createQualifiedTableName(database, subdimTable)
    dimQualifiedTableName = createQualifiedTableName(database, dimTable)
    factQualifiedTableName = createQualifiedTableName(database, factTable)
  }

  after {
    kuduContext.deleteTable(factQualifiedTableName)
    kuduContext.deleteTable(dimQualifiedTableName)
    kuduContext.deleteTable(subQualifiedTableName)
    harness.after()
  }

  test("don't make changes on a dry run") {
    // When
    val SUT = new DatedTableFilter(database, factTable) with KuduStorage

    val result = SUT.doFilter(computeCountsFlag = false, dryRun = true)
    println(Reporting.toYaml(result))
    // Then
    val actual =
      spark.sqlContext.read
        .options(Map("kudu.master" -> kuduMaster, "kudu.table" -> factQualifiedTableName))
        .kudu

    assertResult(3)(actual.count())
  }

  test("delete expired fact table records") {
    // When
    val SUT = new DatedTableFilter(database, factTable) with KuduStorage

    val result = SUT.doFilter(computeCountsFlag = true, dryRun = false)
    println(Reporting.toYaml(result))
    // Then
    val actual =
      spark.sqlContext.read
        .options(Map("kudu.master" -> kuduMaster, "kudu.table" -> factQualifiedTableName))
        .kudu

    assertResult(1)(actual.count())
  }

  test("delete expired dimensional table records") {
    // When
    val SUT = new DatedTableFilter(database, factTable) with KuduStorage

    val result = SUT.doFilter(computeCountsFlag = false)
    println(Reporting.toYaml(result))
    // Then
    val actual =
      spark.sqlContext.read
        .options(Map("kudu.master" -> kuduMaster, "kudu.table" -> dimQualifiedTableName))
        .kudu

    assertResult(1)(actual.count())
  }

  test("delete expired subdimensional table records") {
    // When
    val SUT = new DatedTableFilter(database, factTable) with KuduStorage

    val report = SUT.doFilter(computeCountsFlag = false)
    println(Reporting.toYaml(report))
    // Then
    val actual =
      spark.sqlContext.read
        .options(Map("kudu.master" -> kuduMaster, "kudu.table" -> subQualifiedTableName))
        .kudu

    assertResult(1)(actual.count())
  }

  test("Don't filter out child dataset with a hold") {
    val factTableNameTemp =
      createKuduTable(KuduObjects.defaultFactSchema, KuduObjects.defaultFactKey, kuduContext)
    val dimTableNameTemp =
      createKuduTable(KuduObjects.defaultDimSchema, KuduObjects.defaultDimKey, kuduContext)

    val dimensionTableTemp: ChildTable =
      ChildTable(dimTableNameTemp,
                 "kudu",
                 KuduObjects.defaultFactJoin,
                 Some(Hold(true, "legal", "legal@client.biz")),
                 None)
    val factTableTemp =
      DatedTable(factTableNameTemp, "kudu", "date", 1, None, None, Some(List(dimensionTableTemp)))
    val databaseTemp = Database("impala::default", Seq(factTableTemp))

    insertKuduData(factTableTemp, TestObjects.smallDatasetSeconds, KuduObjects.defaultFactSchema)

    insertKuduData(dimensionTableTemp, KuduObjects.defaultDimData, KuduObjects.defaultDimSchema)

    val newQualifiedTableName = s"${databaseTemp.name}.${dimTableNameTemp}"

    // When
    val SUT = new DatedTableFilter(databaseTemp, factTableTemp) with KuduStorage

    val result = SUT.doFilter(computeCountsFlag = false)
    println(Reporting.toYaml(result))
    // Then
    val resultDf =
      spark.sqlContext.read
        .options(Map("kudu.master" -> kuduMaster, "kudu.table" -> newQualifiedTableName))
        .kudu

    assertResult(3)(resultDf.count())
  }

  test("don't return null on missing table") {
    val table = DatedTable("foo", "kudu", "bar", 1, None, None, None)
    val SUT = new DatedTableFilter(Database("impala::nonexisting", Seq(table)), table)
    with KuduStorage
    val result = SUT.doFilter(computeCountsFlag = true)
    result.head.originalDataset shouldNot be(null)
  }

  /*
   * createKuduTable creates a randomly named Kudu Table
   * And returns the random name it generated for the table
   */
  def createKuduTable(schema: StructType, primaryKey: Seq[String], kc: KuduContext): String = {
    val kuduTableName = UUID.randomUUID().toString().substring(0, 5)

    val qualifiedtableName = s"impala::default.${kuduTableName}"

    if (kc.tableExists(qualifiedtableName)) {
      kc.deleteTable(qualifiedtableName)
    }

    val kuduTableOptions = new CreateTableOptions()
    kuduTableOptions.setRangePartitionColumns(List(primaryKey(0)).asJava).setNumReplicas(1)

    kc.createTable(qualifiedtableName, schema, primaryKey, kuduTableOptions)

    kuduTableName
  }

  /*
   * createQualifiedTableName creates a qualified name for testing
   */
  def createQualifiedTableName(database: Database, table: Table): String = {
    s"${database.name}.${table.name}"
  }

  /*
   * insertKuduData inserts test data into test kudu tables
   */
  def insertKuduData(table: Table, data: List[List[_]], schema: StructType): Unit = {
    val qualifiedTableName = s"impala::default.${table.name}"

    val df = createDataFrame(data, schema)

    kuduContext.insertRows(df, qualifiedTableName)
  }
}
