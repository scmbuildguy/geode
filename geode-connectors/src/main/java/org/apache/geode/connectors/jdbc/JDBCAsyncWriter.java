/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.geode.connectors.jdbc;

import java.util.List;
import java.util.Properties;

import org.apache.geode.cache.asyncqueue.AsyncEvent;
import org.apache.geode.cache.asyncqueue.AsyncEventListener;
import org.apache.geode.cache.query.internal.DefaultQuery;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.pdx.PdxInstance;
import org.apache.logging.log4j.Logger;

/*
 * This class provides write behind cache semantics for a JDBC data source using AsyncEventListener.
 *
 * @since Geode 1.4
 */
public class JDBCAsyncWriter implements AsyncEventListener {

  static final Logger logger = LogService.getLogger();

  private long totalEvents = 0;

  private long successfulEvents = 0;

  private JDBCManager manager;

  @Override
  public void close() {
    // TODO Auto-generated method stub

  }

  @Override
  public boolean processEvents(List<AsyncEvent> events) {
    totalEvents += events.size();
    // TODO: have a better API that lets you do this
    DefaultQuery.setPdxReadSerialized(true);
    try {
      for (AsyncEvent event : events) {
        // TODO: in some cases getDeserializedValue may return non-PdxInstance.
        // In that case need to serialize and deserialize.
        try {
          PdxInstance value = (PdxInstance) event.getDeserializedValue();
          this.manager.write(event.getRegion(), event.getOperation(), event.getKey(), value);
          successfulEvents += 1;
        } catch (RuntimeException ex) {
          // TODO improve the following logging
          logger.error("Exception processing event " + event, ex);
        }
      }
    } finally {
      DefaultQuery.setPdxReadSerialized(false);
    }
    return true;
  }

  @Override
  public void init(Properties props) {
    JDBCConfiguration config = new JDBCConfiguration(props);
    this.manager = new JDBCManager(config);
  };

  public long getTotalEvents() {
    return this.totalEvents;
  }

  public long getSuccessfulEvents() {
    return this.successfulEvents;
  }
}
