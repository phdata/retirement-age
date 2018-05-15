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

import java.sql.Date

object TestObjects {
  val smallDatasetSeconds =
    List(List(Date.valueOf("2018-12-25").getTime / 1000),
         List(Date.valueOf("2017-12-25").getTime / 1000),
         List(Date.valueOf("2016-12-25").getTime / 1000))

  val smallDatasetMillis =
    List(List(Date.valueOf("2018-12-25").getTime),
         List(Date.valueOf("2017-12-25").getTime),
         List(Date.valueOf("2016-12-25").getTime))

  val smallDatasetString =
    List(List(Date.valueOf("2018-12-25")),
         List(Date.valueOf("2017-12-25")),
         List(Date.valueOf("2016-12-25")))

  val parentDataset = List(List(1L, Date.valueOf("2018-12-25").getTime / 1000),
                           List(2L, Date.valueOf("2017-12-25").getTime / 1000),
                           List(3L, Date.valueOf("2016-12-25").getTime / 1000))

  val childDataset = List(List(1L, "record1"), List(2L, "record2"), List(3L, "record3"))
}
