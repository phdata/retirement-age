package io.phdata.retirementage.filters

import com.amazonaws.services.kinesis.model.InvalidArgumentException
import io.phdata.retirementage.domain.{CustomTable, Database}
import org.apache.spark.sql.DataFrame
import org.slf4j.LoggerFactory

abstract class CustomTableFilter(database: Database, table: CustomTable)
    extends TableFilter(database, table) {
  private[filters] val log = LoggerFactory.getLogger(classOf[CustomTableFilter])

  override def expiredRecords(): DataFrame = {
    currentFrame.except(filteredFrame())
  }

  // TODO: this is wrong
  override def filteredFrame(): DataFrame = {
    try {
      var tempFrame = currentFrame
      for (i <- table.filters) {
        val query = s"SELECT * FROM tempFrame WHERE NOT ${i.filter}"
        tempFrame.createOrReplaceTempView("tempFrame")
        log.info(s"Executing SQL Query: " + query)
        tempFrame = tempFrame.sqlContext.sql(query)
      }
      tempFrame
    } catch {
      case _: Throwable => throw new InvalidArgumentException("No filters found")
    }
  }

  override def hasExpiredRecords(): Boolean = !expiredRecords().rdd.isEmpty()
}
