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

package io.phdata.retirementage

import org.joda.time.DateTime

object TestObjects {
  val today: DateTime = DateTime.now
  val smallDatasetSeconds =
    List(List(today.getMillis / 1000),
         List(today.minusYears(1).getMillis / 1000),
         List(today.minusYears(2).getMillis / 1000))

  val smallDatasetMillis =
    List(List(today.getMillis),
         List(today.minusYears(1).getMillis),
         List(today.minusYears(2).getMillis))

  val parentDataset = List(List(1L, today.getMillis / 1000),
                           List(2L, today.minusYears(1).getMillis / 1000),
                           List(3L, today.minusYears(2).getMillis / 1000))

  val childDataset = List(List(1L, "record1"), List(2L, "record2"), List(3L, "record3"))
}
