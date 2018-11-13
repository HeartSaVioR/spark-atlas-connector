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

package com.hortonworks.spark.atlas.sql.testhelper

import com.hortonworks.spark.atlas.AtlasClient
import com.sun.jersey.core.util.MultivaluedMapImpl
import org.apache.atlas.model.instance.AtlasEntity
import org.apache.atlas.model.typedef.AtlasTypesDef

import scala.collection.mutable

class EntitiesModificationTrackingAtlasClient extends AtlasClient {
  val createdEntities = new mutable.ListBuffer[AtlasEntity]()
  val updatedEntities = new mutable.ListBuffer[(String, String, AtlasEntity)]()
  val deletedEntities = new mutable.ListBuffer[(String, String)]()

  def clear(): Unit = {
    createdEntities.clear()
    updatedEntities.clear()
    deletedEntities.clear()
  }

  override def createAtlasTypeDefs(typeDefs: AtlasTypesDef): Unit = {}

  override def getAtlasTypeDefs(searchParams: MultivaluedMapImpl): AtlasTypesDef = {
    new AtlasTypesDef()
  }

  override def updateAtlasTypeDefs(typeDefs: AtlasTypesDef): Unit = {}

  override protected def doCreateEntities(entities: Seq[AtlasEntity]): Unit = {
    createdEntities ++= entities
  }

  override protected def doDeleteEntityWithUniqueAttr(entityType: String,
                                                      attribute: String): Unit = {
    deletedEntities += ((entityType, attribute))
  }

  override protected def doUpdateEntityWithUniqueAttr(entityType: String, attribute: String,
                                                      entity: AtlasEntity): Unit = {
    updatedEntities += ((entityType, attribute, entity))
  }
}