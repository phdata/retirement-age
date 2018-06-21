/* Copyright 2018 phData Inc. */

package io.phdata.retirementage.loadgen

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.scalatest.FunSuite

class LoadGeneratorTest extends FunSuite {
  val conf  = new SparkConf().setMaster("local[*]")
  val spark = SparkSession.builder().enableHiveSupport().config(conf)

  test("Joining fact table to dimension table results in records > 0") {
    fail()
  }

  test("Joining dimension table to subdimension table results in records > 0") {
    fail()
  }

  test("Create test dataframe with specified payload and record size") {
    fail()
  }

  test("Create test dataframe with specified payload, record size, and foreign keys to parent table") {
    fail()
  }
}
