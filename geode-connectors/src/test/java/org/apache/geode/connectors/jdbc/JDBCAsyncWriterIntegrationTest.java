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

import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.assertj.core.api.Assertions.*;

import java.sql.*;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.junit.*;
import org.junit.experimental.categories.Category;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.connectors.jdbc.JDBCAsyncWriter;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.pdx.PdxInstance;
import org.apache.geode.test.junit.categories.IntegrationTest;
import org.awaitility.Awaitility;

@Category(IntegrationTest.class)
public class JDBCAsyncWriterIntegrationTest {

  private Cache cache;

  private Connection conn;

  private Statement stmt;

  JDBCAsyncWriter jdbcWriter;

  private String dbName = "DerbyDB";

  private String regionTableName = "employees";

  private String driver = "org.apache.derby.jdbc.EmbeddedDriver";

  private String connectionURL = "jdbc:derby:memory:" + dbName + ";create=true";

  @Before
  public void setup() throws Exception {
    try {
      cache = CacheFactory.getAnyInstance();
    } catch (Exception e) {
      // ignore
    }
    if (null == cache) {
      cache = (GemFireCacheImpl) new CacheFactory().setPdxReadSerialized(true).set(MCAST_PORT, "0")
          .create();
    }
    setupDB();
  }

  @After
  public void tearDown() throws Exception {
    if (cache != null && !cache.isClosed()) {
      cache.close();
      cache = null;
    }
    closeDB();
  }

  public void setupDB() throws Exception {
    Class.forName(driver);
    conn = DriverManager.getConnection(connectionURL);
    stmt = conn.createStatement();
    stmt.execute("Create Table " + regionTableName
        + " (id varchar(10) primary key not null, name varchar(10), age int)");
  }

  public void closeDB() throws Exception {
    if (stmt == null) {
      stmt = conn.createStatement();
    }
    stmt.execute("Drop table " + regionTableName);
    stmt.close();

    if (conn != null) {
      conn.close();
    }
  }

  private Properties getRequiredProperties() {
    Properties props = new Properties();
    props.setProperty("driver", this.driver);
    props.setProperty("url", this.connectionURL);
    return props;
  }

  @Test
  public void canExecuteSQLOnDataBase() throws Exception {
    stmt.execute("Insert into " + regionTableName + " values ('1', 'emp1', 10)");
    stmt.execute("Select * from " + regionTableName);
    DatabaseMetaData metaData = conn.getMetaData();
    ResultSet tablesRS = metaData.getTables(null, null, "%", null);
    String realTableName = null;
    while (tablesRS.next()) {
      String name = tablesRS.getString("TABLE_NAME");
      if (name.equalsIgnoreCase(this.regionTableName)) {
        if (realTableName != null) {
          throw new IllegalStateException("Duplicate tables that match region name");
        }
        realTableName = name;
      }
    }
    if (realTableName == null) {
      throw new IllegalStateException("no table was found that matches " + regionTableName);
    }
    ResultSet primaryKeys = metaData.getPrimaryKeys(null, null, realTableName);
  }

  @Test
  public void canInstallJDBCAsyncWriterOnRegion() {
    Region employees = createRegionWithJDBCAsyncWriter(regionTableName, getRequiredProperties());
    PdxInstance pdx1 = cache.createPdxInstanceFactory("Employee").writeString("name", "Emp1")
        .writeInt("age", 55).create();
    PdxInstance pdx2 = cache.createPdxInstanceFactory("Employee").writeString("name", "Emp2")
        .writeInt("age", 21).create();
    employees.put("1", pdx1);
    employees.put("2", pdx2);

    Awaitility.await().atMost(30, TimeUnit.SECONDS)
        .until(() -> assertThat(jdbcWriter.getTotalEvents()).isEqualTo(2));

  }

  @Test
  public void jdbcAsyncWriterCanInsertIntoDatabase() throws Exception {
    Region employees = createRegionWithJDBCAsyncWriter(regionTableName, getRequiredProperties());
    PdxInstance pdx1 = cache.createPdxInstanceFactory("Employee").writeString("name", "Emp1")
        .writeInt("age", 55).writeInt("id", 3).create();
    PdxInstance pdx2 = cache.createPdxInstanceFactory("Employee").writeString("name", "Emp2")
        .writeInt("age", 21).create();
    employees.put("1", pdx1);
    employees.put("2", pdx2);

    Awaitility.await().atMost(30, TimeUnit.SECONDS)
        .until(() -> assertThat(jdbcWriter.getSuccessfulEvents()).isEqualTo(2));

    validateTableRowCount(2);
    printTable();
  }

  @Test
  public void jdbcAsyncWriterCanDestroyFromDatabase() throws Exception {
    Region employees = createRegionWithJDBCAsyncWriter(regionTableName, getRequiredProperties());
    PdxInstance pdx1 = cache.createPdxInstanceFactory("Employee").writeString("name", "Emp1")
        .writeInt("age", 55).create();
    PdxInstance pdx2 = cache.createPdxInstanceFactory("Employee").writeString("name", "Emp2")
        .writeInt("age", 21).create();
    employees.put("1", pdx1);
    employees.put("2", pdx2);

    Awaitility.await().atMost(30, TimeUnit.SECONDS)
        .until(() -> assertThat(jdbcWriter.getSuccessfulEvents()).isEqualTo(2));

    employees.destroy("1");

    Awaitility.await().atMost(30, TimeUnit.SECONDS)
        .until(() -> assertThat(jdbcWriter.getSuccessfulEvents()).isEqualTo(3));

    validateTableRowCount(1);
    printTable();
  }

  @Test
  public void jdbcAsyncWriterCanUpdateDatabase() throws Exception {
    Region employees = createRegionWithJDBCAsyncWriter(regionTableName, getRequiredProperties());
    PdxInstance pdx1 = cache.createPdxInstanceFactory("Employee").writeString("name", "Emp1")
        .writeInt("age", 55).create();
    PdxInstance pdx2 = cache.createPdxInstanceFactory("Employee").writeString("name", "Emp2")
        .writeInt("age", 21).create();
    employees.put("1", pdx1);
    employees.put("2", pdx2);

    Awaitility.await().atMost(30, TimeUnit.SECONDS)
        .until(() -> assertThat(jdbcWriter.getSuccessfulEvents()).isEqualTo(2));

    PdxInstance pdx3 = cache.createPdxInstanceFactory("Employee").writeString("name", "Emp1")
        .writeInt("age", 72).create();
    employees.put("1", pdx3);

    Awaitility.await().atMost(30, TimeUnit.SECONDS)
        .until(() -> assertThat(jdbcWriter.getSuccessfulEvents()).isEqualTo(3));

    validateTableRowCount(2);
    printTable();
  }

  private Region createRegionWithJDBCAsyncWriter(String regionName, Properties props) {
    jdbcWriter = new JDBCAsyncWriter();
    jdbcWriter.init(props);
    cache.createAsyncEventQueueFactory().setBatchSize(1).setBatchTimeInterval(1)
        .create("jdbcAsyncQueue", jdbcWriter);

    RegionFactory rf = cache.createRegionFactory(RegionShortcut.REPLICATE);
    rf.addAsyncEventQueueId("jdbcAsyncQueue");
    return rf.create(regionName);
  }

  private void validateTableRowCount(int expected) throws Exception {
    ResultSet rs = stmt.executeQuery("select count(*) from " + regionTableName);
    rs.next();
    int size = rs.getInt(1);
    assertThat(size).isEqualTo(expected);
  }

  private void printTable() throws Exception {
    ResultSet rs = stmt.executeQuery("select * from " + regionTableName);
    ResultSetMetaData rsmd = rs.getMetaData();
    int columnsNumber = rsmd.getColumnCount();
    while (rs.next()) {
      for (int i = 1; i <= columnsNumber; i++) {
        if (i > 1)
          System.out.print(",  ");
        String columnValue = rs.getString(i);
        System.out.print(rsmd.getColumnName(i) + ": " + columnValue);
      }
      System.out.println("");
    }
  }

}
