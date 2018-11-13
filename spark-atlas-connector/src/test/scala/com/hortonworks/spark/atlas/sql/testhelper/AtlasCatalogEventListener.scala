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

import org.apache.spark.scheduler.{SparkListener, SparkListenerEvent}
import org.apache.spark.sql.catalyst.catalog.ExternalCatalogEvent

import scala.collection.mutable

class AtlasCatalogEventListener extends SparkListener {
  val catalogEvents = new mutable.MutableList[ExternalCatalogEvent]()

  override def onOtherEvent(event: SparkListenerEvent): Unit = {
    // We only care about SQL related events.
    event match {
      case e: ExternalCatalogEvent => catalogEvents += e
      case _ => // Ignore other events
    }
  }

  def clear(): Unit = {
    catalogEvents.clear()
  }
}
