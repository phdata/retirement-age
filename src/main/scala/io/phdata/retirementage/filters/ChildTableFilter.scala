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

package io.phdata.retirementage.filters

import io.phdata.retirementage.domain.{ChildTable, Database, JoinOn}
import org.apache.spark.sql.{Column, DataFrame}

/**
  * Filter for a dataset not containing a date column but that is joined from a table that does.
  * @inheritdoc
  * @param parent The parent dataset that is joined from
  */
abstract class ChildTableFilter(database: Database, table: ChildTable, parent: TableFilter)
    extends TableFilter(database, table) {

  override def filteredFrame(): DataFrame = {
    filter("leftanti")
  }

  /**
    * @inheritdoc
    */
  override def hasExpiredRecords(): Boolean = !expiredRecords().rdd.isEmpty()

  /**
    * @inheritdoc
    */
  override def expiredRecords(): DataFrame = {
    filter("leftsemi")
  }

  private[filters] def filter(joinType: String): DataFrame = {
    val joinExpr = joinExpression(table.join_on)

    currentFrame.join(parent.expiredRecords(), joinExpr, joinType)
  }

  private[filters] def joinExpression(joins: List[JoinOn]): Column = {
    joins
      .map(
        joinKeys =>
          currentFrame(joinKeys.self) === parent
            .expiredRecords()(joinKeys.parent))
      .reduceLeft(_ && _)
  }
}
