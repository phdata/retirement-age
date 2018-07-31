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

import io.phdata.retirementage.domain.{ChildTable, Database}
import io.phdata.retirementage.storage.{HdfsStorage, StorageActions}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col

/**
  * Filter for a dataset not containing a date column but that is joined from a table that does.
  * @inheritdoc
  * @param parent The parent dataset that is joined from
  */
abstract class ChildTableFilter(database: Database, table: ChildTable, parent: TableFilter)
    extends TableFilter(database, table) {

  override def filteredFrame = {
    val joinKeys = table.join_on

    currentFrame
      .join(parent.expiredRecords(),
            currentFrame.col(joinKeys.self)
              === parent.expiredRecords.col(joinKeys.parent),
            "leftanti")
  }

  /**
    * @inheritdoc
    */
  override def hasExpiredRecords(): Boolean =
    expiredRecords().count() > 0

  /**
    * @inheritdoc
    */
  override def expiredRecords(): DataFrame = {
    val joinKeys = table.join_on
    currentFrame.join(
      parent.expiredRecords(),
      currentFrame.col(joinKeys.self) === parent.expiredRecords.col(joinKeys.parent),
      "inner")
  }
}
