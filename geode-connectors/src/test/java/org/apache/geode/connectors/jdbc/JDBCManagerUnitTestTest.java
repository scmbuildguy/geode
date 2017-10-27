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

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Properties;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.Operation;
import org.apache.geode.cache.Region;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.pdx.PdxInstance;
import org.apache.geode.pdx.PdxInstanceFactory;
import org.apache.geode.pdx.internal.PdxInstanceImpl;
import org.apache.geode.pdx.internal.PdxType;
import org.apache.geode.test.fake.Fakes;
import org.apache.geode.test.junit.categories.UnitTest;

@Category(UnitTest.class)
public class JDBCManagerUnitTestTest {

  private JDBCManager mgr;
  private String regionName = "jdbcRegion";

  public class TestableJDBCManager extends JDBCManager {

    TestableJDBCManager(JDBCConfiguration config) {
      super(config);
    }

    @Override
    protected Connection createConnection(String url) throws SQLException {
      ResultSet rsKeys = mock(ResultSet.class);
      when(rsKeys.next()).thenReturn(true, false);
      when(rsKeys.getString("COLUMN_NAME")).thenReturn("ID");

      ResultSet rs = mock(ResultSet.class);
      when(rs.next()).thenReturn(true, false);
      when(rs.getString("TABLE_NAME")).thenReturn(regionName.toUpperCase());

      DatabaseMetaData metaData = mock(DatabaseMetaData.class);
      when(metaData.getPrimaryKeys(null, null, regionName.toUpperCase())).thenReturn(rsKeys);
      when(metaData.getTables(any(), any(), any(), any())).thenReturn(rs);

      PreparedStatement pstmt = mock(PreparedStatement.class);
      when(pstmt.getUpdateCount()).thenReturn(1);

      Connection conn = mock(Connection.class);
      when(conn.getMetaData()).thenReturn(metaData);
      when(conn.prepareStatement(any())).thenReturn(pstmt, null);

      return conn;
    }

  }

  @Before
  public void setUp() throws Exception {
    Properties props = new Properties();
    String driver = "org.apache.derby.jdbc.EmbeddedDriver";
    String connectionURL = "jdbc:derby:memory:DerbyTestDB;create=true";
    props.setProperty("url", connectionURL);
    props.setProperty("driver", driver);
    JDBCConfiguration config = new JDBCConfiguration(props);
    this.mgr = new TestableJDBCManager(config);
  }

  @After
  public void tearDown() throws Exception {}

  @Test
  public void testWritingCreate() {
    GemFireCacheImpl cache = Fakes.cache();
    Region region = Fakes.region(regionName, cache);
    PdxInstanceImpl pdx1 = mock(PdxInstanceImpl.class);
    when(pdx1.getFieldNames()).thenReturn(Arrays.asList("name", "age"));
    when(pdx1.getField("name")).thenReturn("Emp1");
    when(pdx1.getField("age")).thenReturn(21);
    PdxType pdxType = mock(PdxType.class);
    when(pdxType.getTypeId()).thenReturn(1);
    when(pdx1.getPdxType()).thenReturn(pdxType);
    this.mgr.write(region, Operation.CREATE, "1", pdx1);
  }

}
