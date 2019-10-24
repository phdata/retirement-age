package io.phdata.retirementage

import io.phdata.retirementage.domain.JoinOn
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}

object KuduObjects {
  val defaultFactJoin = JoinOn("date", "date")
  val defaultDimJoin  = JoinOn("id", "id")

  // Default fact objects
  val defaultFactSchema = StructType(StructField("date", LongType, nullable = false) :: Nil)

  val defaultFactKey: Seq[String] = Seq("date")
  val defaultFactData: List[List[Long]] = TestObjects.smallDatasetSeconds

  // Default dimension objects
  val defaultDimSchema = StructType(
    StructField("id", StringType, nullable = false) :: StructField("date", LongType, nullable = false) :: Nil)

  val defaultDimKey: Seq[String] = Seq("id")

  val defaultDimData = List(List("1", TestObjects.today.getMillis / 1000),
                            List("2", TestObjects.today.minusYears(1).getMillis / 1000),
                            List("3", TestObjects.today.minusYears(2).getMillis / 1000))

  //Default sub-dimension objects
  val defaultSubSchema = StructType(
    StructField("sub_id", StringType, nullable = false) :: StructField("id", StringType, nullable = false) :: Nil)

  val defaultSubKey: Seq[String] = Seq("sub_id")

  val defaultSubData = List(List("10", "1"), List("20", "2"), List("30", "3"))
}
