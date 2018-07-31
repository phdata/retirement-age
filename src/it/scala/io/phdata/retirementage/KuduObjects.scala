package io.phdata.retirementage

import java.sql.Date

import io.phdata.retirementage.domain.JoinOn
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}

object KuduObjects {
  val defaultFactJoin = JoinOn("date", "date")
  val defaultDimJoin  = JoinOn("id", "id")

  // Default fact objects
  val defaultFactSchema = StructType(StructField("date", LongType, false) :: Nil)

  val defaultFactKey = Seq("date")

  // Default dimension objects
  val defaultDimSchema = StructType(
    StructField("id", StringType, false) :: StructField("date", LongType, false) :: Nil)

  val defaultDimKey = Seq("id")

  val defaultDimData = List(List("1", Date.valueOf("2018-12-25").getTime / 1000),
                            List("2", Date.valueOf("2017-12-25").getTime / 1000),
                            List("3", Date.valueOf("2016-12-25").getTime / 1000))

  //Default sub-dimension objects
  val defaultSubSchema = StructType(
    StructField("sub_id", StringType, false) :: StructField("id", StringType, false) :: Nil)

  val defaultSubKey = Seq("sub_id")

  val defaultSubData = List(List("10", "1"), List("20", "2"), List("30", "3"))
}
