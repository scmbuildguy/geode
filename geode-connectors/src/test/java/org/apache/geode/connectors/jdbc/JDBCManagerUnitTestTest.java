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
import static com.googlecode.catchexception.CatchException.*;
import static com.googlecode.catchexception.CatchException.caughtException;

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
import org.mockito.ArgumentCaptor;

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
  Connection connection;
  PreparedStatement preparedStatement;

  private static final String ID_COLUMN_NAME = "ID";
  private static final String NAME_COLUMN_NAME = "name";
  private static final String AGE_COLUMN_NAME = "age";

  public class TestableJDBCManager extends JDBCManager {

    TestableJDBCManager(JDBCConfiguration config) {
      super(config);
    }

    @Override
    protected Connection createConnection(String url) throws SQLException {
      ResultSet rsKeys = mock(ResultSet.class);
      when(rsKeys.next()).thenReturn(true, false);
      when(rsKeys.getString("COLUMN_NAME")).thenReturn(ID_COLUMN_NAME);

      ResultSet rs = mock(ResultSet.class);
      when(rs.next()).thenReturn(true, false);
      when(rs.getString("TABLE_NAME")).thenReturn(regionName.toUpperCase());

      DatabaseMetaData metaData = mock(DatabaseMetaData.class);
      when(metaData.getPrimaryKeys(null, null, regionName.toUpperCase())).thenReturn(rsKeys);
      when(metaData.getTables(any(), any(), any(), any())).thenReturn(rs);

      preparedStatement = mock(PreparedStatement.class);
      when(preparedStatement.getUpdateCount()).thenReturn(1);

      connection = mock(Connection.class);
      when(connection.getMetaData()).thenReturn(metaData);
      when(connection.prepareStatement(any())).thenReturn(preparedStatement, null);

      return connection;
    }

  }

  @Before
  public void setUp() throws Exception {}

  @After
  public void tearDown() throws Exception {}

  private void createManager(String driver, String url) {
    Properties props = new Properties();
    props.setProperty("url", url);
    props.setProperty("driver", driver);
    JDBCConfiguration config = new JDBCConfiguration(props);
    this.mgr = new TestableJDBCManager(config);
  }

  private void createDefaultManager() {
    createManager("java.lang.String", "fakeURL");
  }

  @Test
  public void verifySimpleCreateCallsExecute() throws SQLException {
    createDefaultManager();
    GemFireCacheImpl cache = Fakes.cache();
    Region region = Fakes.region(regionName, cache);
    PdxInstanceImpl pdx1 = mockPdxInstance("Emp1", 21);
    this.mgr.write(region, Operation.CREATE, "1", pdx1);
    verify(this.preparedStatement).execute();
    ArgumentCaptor<String> sqlCaptor = ArgumentCaptor.forClass(String.class);
    verify(this.connection).prepareStatement(sqlCaptor.capture());
    assertThat(sqlCaptor.getValue()).isEqualTo("INSERT INTO " + regionName + "(" + NAME_COLUMN_NAME
        + ", " + AGE_COLUMN_NAME + ", " + ID_COLUMN_NAME + ") VALUES (?,?,?)");
  }

  @Test
  public void verifySimpleUpdateCallsExecute() throws SQLException {
    createDefaultManager();
    GemFireCacheImpl cache = Fakes.cache();
    Region region = Fakes.region(regionName, cache);
    PdxInstanceImpl pdx1 = mockPdxInstance("Emp1", 21);
    this.mgr.write(region, Operation.UPDATE, "1", pdx1);
    verify(this.preparedStatement).execute();
    ArgumentCaptor<String> sqlCaptor = ArgumentCaptor.forClass(String.class);
    verify(this.connection).prepareStatement(sqlCaptor.capture());
    assertThat(sqlCaptor.getValue()).isEqualTo("UPDATE " + regionName + " SET " + NAME_COLUMN_NAME
        + " = ?, " + AGE_COLUMN_NAME + " = ? WHERE " + ID_COLUMN_NAME + " = ?");
  }

  @Test
  public void verifySimpleDestroyCallsExecute() throws SQLException {
    createDefaultManager();
    GemFireCacheImpl cache = Fakes.cache();
    Region region = Fakes.region(regionName, cache);
    this.mgr.write(region, Operation.DESTROY, "1", null);
    verify(this.preparedStatement).execute();
    ArgumentCaptor<String> sqlCaptor = ArgumentCaptor.forClass(String.class);
    verify(this.connection).prepareStatement(sqlCaptor.capture());
    assertThat(sqlCaptor.getValue())
        .isEqualTo("DELETE FROM " + regionName + " WHERE " + ID_COLUMN_NAME + " = ?");
  }

  @Test
  public void verifyTwoCreatesReuseSameStatement() throws SQLException {
    createDefaultManager();
    GemFireCacheImpl cache = Fakes.cache();
    Region region = Fakes.region(regionName, cache);
    PdxInstanceImpl pdx1 = mockPdxInstance("Emp1", 21);
    PdxInstanceImpl pdx2 = mockPdxInstance("Emp2", 55);
    this.mgr.write(region, Operation.CREATE, "1", pdx1);
    this.mgr.write(region, Operation.CREATE, "2", pdx2);
    verify(this.preparedStatement, times(2)).execute();
    verify(this.connection).prepareStatement(any());
  }

  private PdxInstanceImpl mockPdxInstance(String name, int age) {
    PdxInstanceImpl pdxInstance = mock(PdxInstanceImpl.class);
    when(pdxInstance.getFieldNames()).thenReturn(Arrays.asList(NAME_COLUMN_NAME, AGE_COLUMN_NAME));
    when(pdxInstance.getField(NAME_COLUMN_NAME)).thenReturn(name);
    when(pdxInstance.getField(AGE_COLUMN_NAME)).thenReturn(age);
    PdxType pdxType = mock(PdxType.class);
    when(pdxType.getTypeId()).thenReturn(1);
    when(pdxInstance.getPdxType()).thenReturn(pdxType);
    return pdxInstance;
  }

  @Test
  public void verifyMissingDriverClass() {
    createManager("non existent driver", "fakeURL");
    catchException(this.mgr).getConnection();
    assertThat((Exception) caughtException()).isInstanceOf(IllegalStateException.class);
    assertThat(caughtException().getMessage())
        .isEqualTo("Driver class \"non existent driver\" not found");
  }

  @Test
  public void verifySimpleInvalidateIsUnsupported() throws SQLException {
    createDefaultManager();
    GemFireCacheImpl cache = Fakes.cache();
    Region region = Fakes.region(regionName, cache);
    PdxInstanceImpl pdx1 = mockPdxInstance("Emp1", 21);
    catchException(this.mgr).write(region, Operation.INVALIDATE, "1", pdx1);
    assertThat((Exception) caughtException()).isInstanceOf(IllegalStateException.class);
    assertThat(caughtException().getMessage()).isEqualTo("unsupported operation INVALIDATE");
  }

}
