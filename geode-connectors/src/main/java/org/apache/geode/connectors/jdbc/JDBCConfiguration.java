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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;

public class JDBCConfiguration {

  private static final String DRIVER = "driver";

  private static final String URL = "url";

  private static final String USER = "user";

  private static final String PASSWORD = "password";

  private static final List<String> knownProperties =
      Collections.unmodifiableList(Arrays.asList(DRIVER, URL, USER, PASSWORD));

  private static final List<String> requiredProperties =
      Collections.unmodifiableList(Arrays.asList(DRIVER, URL));

  private final String driver;
  private final String url;
  private final String user;
  private final String password;

  JDBCConfiguration(Properties configProps) {
    validateKnownProperties(configProps);
    validateRequiredProperties(configProps);
    this.driver = configProps.getProperty(DRIVER);
    this.url = configProps.getProperty(URL);
    this.user = configProps.getProperty(USER);
    this.password = configProps.getProperty(PASSWORD);
  }

  private void validateKnownProperties(Properties configProps) {
    Set<Object> keys = new HashSet<>(configProps.keySet());
    keys.removeAll(knownProperties);
    if (!keys.isEmpty()) {
      throw new IllegalArgumentException("unknown properties: " + keys);
    }
  }

  private void validateRequiredProperties(Properties configProps) {
    List<String> reqKeys = new ArrayList<>(requiredProperties);
    reqKeys.removeAll(configProps.keySet());
    if (!reqKeys.isEmpty()) {
      Collections.sort(reqKeys);
      throw new IllegalArgumentException("missing required properties: " + reqKeys);
    }
  }

  public String getDriver() {
    return this.driver;
  }

  public String getURL() {
    return this.url;
  }

  public String getUser() {
    return this.user;
  }

  public String getPassword() {
    return this.password;
  }

}
