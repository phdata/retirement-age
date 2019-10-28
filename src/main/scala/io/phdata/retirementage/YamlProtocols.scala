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

import io.phdata.retirementage.domain._
import net.jcazevedo.moultingyaml._
import org.slf4j.LoggerFactory

/**
  * Yaml protocols for parsing yaml configuration into domain objects using MoultingYaml.
  *
  */
object YamlProtocols extends DefaultYamlProtocol {
  private val log = LoggerFactory.getLogger(YamlProtocols.getClass.getName)

  implicit val joinKeys = yamlFormat2(JoinOn)
  // Make the yamlformat lazy to allow for recursive Table types
  implicit val datedTable: YamlFormat[DatedTable]   = lazyFormat(yamlFormat7(DatedTable))
  implicit val relatedTable: YamlFormat[ChildTable] = lazyFormat(yamlFormat5(ChildTable))
  implicit val hold                                 = yamlFormat3(Hold)

  implicit val filterFormat: YamlFormat[CustomFilter] = lazyFormat(yamlFormat1(CustomFilter))
  implicit val customTable: YamlFormat[CustomTable]   = lazyFormat(yamlFormat5(CustomTable))
  implicit val tableFormat: YamlFormat[Table]         = TableYamlFormat

  implicit val databaseFormat = yamlFormat2(Database)
  implicit val configFormat   = yamlFormat2(Config)
  implicit val datasetReport  = yamlFormat2(DatasetReport)
  implicit val resultFormat   = yamlFormat5(RetirementReport)

  implicit object TableYamlFormat extends YamlFormat[Table] {
    def write(t: Table) = {
      t match {
        case d: DatedTable =>
          YamlObject(
            YamlString("name")              -> YamlString(d.name),
            YamlString("storage_type")      -> YamlString(d.storage_type),
            YamlString("expiration_column") -> YamlString(d.expiration_column),
            YamlString("expiration_days")   -> YamlNumber(d.expiration_days),
            YamlString("hold")              -> d.hold.toYaml,
            YamlString("date_format_string") -> (if (d.date_format_string.isDefined) {
                                                   YamlString(d.date_format_string.get)
                                                 } else {
                                                   YamlNull
                                                 }),
            YamlString("child_tables") -> d.child_tables.toYaml
          )
        case c: CustomTable =>
          YamlObject(
            YamlString("name")         -> YamlString(c.name),
            YamlString("storage_type") -> YamlString(c.storage_type),
            YamlString("filters")      -> c.filters.toYaml,
            YamlString("hold")         -> c.hold.toYaml,
            YamlString("child_tables") -> c.child_tables.toYaml
          )
      }
    }

    def read(value: YamlValue) = {
      //TODO: Correctly handle exceptions
      try {
        value.asYamlObject.convertTo[DatedTable]
      } catch {
        case t: Throwable =>
          log.debug("Error parsing config as DatedTable", t)
          log.warn(s"Unable to read table config as DatedTable, attempting CustomTable: $value")
          value.asYamlObject.convertTo[CustomTable]
      }

    }

  }

}
