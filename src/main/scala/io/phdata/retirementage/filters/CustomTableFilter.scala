package io.phdata.retirementage.filters

import com.amazonaws.services.kinesis.model.InvalidArgumentException
import io.phdata.retirementage.domain.{CustomTable, Database}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

abstract class CustomTableFilter(database: Database, table: CustomTable)
    extends TableFilter(database, table) {

  override def expiredRecords(): DataFrame = {
    currentFrame.except(filteredFrame())
  }

  override def filteredFrame() = {
    try {
      var tempFrame = currentFrame
      for (i <- table.filters) {
        val query = s"SELECT * FROM tempFrame WHERE NOT ${i.filter}"
        tempFrame.createOrReplaceTempView("tempFrame")
        log(s"Executing SQL Query: " + query)
        tempFrame = tempFrame.sqlContext.sql(query)
      }
      tempFrame
    } catch {
      case _: Throwable => throw new InvalidArgumentException("No filters found")
    }
  }

  override def hasExpiredRecords(): Boolean = true
}
