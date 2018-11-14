/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hortonworks.spark.atlas.sql

import com.hortonworks.spark.atlas.sql.testhelper.{AtlasCatalogEventListener, EntitiesModificationTrackingAtlasClient}
import com.hortonworks.spark.atlas.types.external
import com.hortonworks.spark.atlas.{AtlasClient, AtlasClientConf, WithHiveSupport}
import org.apache.spark.sql.catalyst.catalog.ExternalCatalogEvent
import org.scalatest.FunSuite

class SparkSqlDdlQuerySuite extends FunSuite with WithHiveSupport {
  import com.hortonworks.spark.atlas.sql.testhelper.AtlasEntityReadHelper._

  val atlasClientConf: AtlasClientConf = new AtlasClientConf()
    .set(AtlasClientConf.CHECK_MODEL_IN_START.key, "false")
  var atlasClient: EntitiesModificationTrackingAtlasClient = _
  val testHelperCatalogListener = new AtlasCatalogEventListener()

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    atlasClient = new EntitiesModificationTrackingAtlasClient()
    testHelperCatalogListener.clear()
    sparkSession.sparkContext.addSparkListener(testHelperCatalogListener)
  }

  override protected def afterAll(): Unit = {
    atlasClient = null
    sparkSession.sparkContext.removeSparkListener(testHelperCatalogListener)
    super.afterAll()
  }

  class DirectProcessSparkCatalogEventProcessor(
      atlasClient: AtlasClient,
      atlasClientConf: AtlasClientConf)
    extends SparkCatalogEventProcessor(atlasClient, atlasClientConf) {

    override def process(e: ExternalCatalogEvent): Unit = super.process(e)
  }

  test("Add column in table via spark sql query - Hive table") {
    val catalogProcessor = new DirectProcessSparkCatalogEventProcessor(atlasClient, atlasClientConf)

    sparkSession.sql("create table test1(col1 int)")

    val catalogEvents = testHelperCatalogListener.catalogEvents
    catalogEvents.foreach(catalogProcessor.process)

    // assert for 'create table' operation
    val tablesCreated = atlasClient.createdEntities.filter(_.getTypeName == external.HIVE_TABLE_TYPE_STRING)
    assert(tablesCreated.size === 1)
    val tableCreated = tablesCreated.head
    assert(getStringAttribute(tableCreated, "qualifiedName") === s"default.test1@primary")

    // determine whether the table has attribute for columns as 'columns' or 'spark_schema'
    val attrNameForColumns = {
      if (tableCreated.getAttributes.containsKey("columns")) {
        "columns"
      } else if (tableCreated.getAttributes.containsKey("spark_schema")) {
        "spark_schema"
      } else {
        fail("Table entity doesn't have valid attribute for column representation.")
      }
    }

    testHelperCatalogListener.clear()
    atlasClient.clear()

    sparkSession.sql("alter table test1 add COLUMNS (col2 int)")

    val catalogEvents2 = testHelperCatalogListener.catalogEvents
    catalogEvents2.foreach(catalogProcessor.process)
    val createdEntities = atlasClient.createdEntities
    val updatedEntities = atlasClient.updatedEntities

    // SAC doesn't know which columns are modified, so create requests will be occurred
    // for all columns
    val expectedColumnNames = Set("col1", "col2")
    val expectedQualifiedName = expectedColumnNames.map(name => s"default.test1.$name@primary")
    val expectedType = "integer"

    assert(createdEntities.size === 2)
    assert(createdEntities.forall(_.getTypeName == external.HIVE_COLUMN_TYPE_STRING))
    assert(createdEntities.forall(getStringAttribute(_, "type") == expectedType))
    assert(createdEntities.map(getStringAttribute(_, "name")).toSet == expectedColumnNames)
    assert(createdEntities.map(getStringAttribute(_, "qualifiedName")).toSet ==
      expectedQualifiedName)

    assert(updatedEntities.size === 1)
    val updatedEntity = updatedEntities.head

    val columns = getSeqAtlasEntityAttribute(updatedEntity._3, attrNameForColumns)
    assert(columns.toSet === createdEntities.toSet)
  }

}
