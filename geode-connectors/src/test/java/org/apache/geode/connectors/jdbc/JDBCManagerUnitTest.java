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
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.ArgumentCaptor;

import org.apache.geode.cache.Operation;
import org.apache.geode.cache.Region;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.pdx.internal.PdxInstanceImpl;
import org.apache.geode.pdx.internal.PdxType;
import org.apache.geode.test.fake.Fakes;
import org.apache.geode.test.junit.categories.UnitTest;

@Category(UnitTest.class)
public class JDBCManagerUnitTest {

  private JDBCManager mgr;
  private String regionName = "jdbcRegion";
  Connection connection;
  PreparedStatement preparedStatement;
  PreparedStatement preparedStatement2;

  private static final String ID_COLUMN_NAME = "ID";
  private static final String NAME_COLUMN_NAME = "name";
  private static final String AGE_COLUMN_NAME = "age";

  public class TestableUpsertJDBCManager extends JDBCManager {

    final int upsertReturn;

    TestableUpsertJDBCManager(JDBCConfiguration config) {
      super(config);
      upsertReturn = 1;
    }

    TestableUpsertJDBCManager(JDBCConfiguration config, int upsertReturn) {
      super(config);
      this.upsertReturn = upsertReturn;
    }

    @Override
    protected Connection createConnection(String url, String user, String password)
        throws SQLException {
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
      preparedStatement2 = mock(PreparedStatement.class);
      when(preparedStatement.getUpdateCount()).thenReturn(0);
      when(preparedStatement2.getUpdateCount()).thenReturn(this.upsertReturn);

      connection = mock(Connection.class);
      when(connection.getMetaData()).thenReturn(metaData);
      when(connection.prepareStatement(any())).thenReturn(preparedStatement, preparedStatement2);

      return connection;
    }
  }

  public class TestableJDBCManagerWithResultSets extends JDBCManager {

    private ResultSet tableResults;
    private ResultSet primaryKeyResults;

    TestableJDBCManagerWithResultSets(JDBCConfiguration config, ResultSet tableResults,
        ResultSet primaryKeyResults) {
      super(config);
      this.tableResults = tableResults;
      this.primaryKeyResults = primaryKeyResults;
    }

    @Override
    protected Connection createConnection(String url, String user, String password)
        throws SQLException {
      if (primaryKeyResults == null) {
        primaryKeyResults = mock(ResultSet.class);
        when(primaryKeyResults.next()).thenReturn(true, false);
        when(primaryKeyResults.getString("COLUMN_NAME")).thenReturn(ID_COLUMN_NAME);
      }

      if (tableResults == null) {
        tableResults = mock(ResultSet.class);
        when(tableResults.next()).thenReturn(true, false);
        when(tableResults.getString("TABLE_NAME")).thenReturn(regionName.toUpperCase());
      }

      DatabaseMetaData metaData = mock(DatabaseMetaData.class);
      when(metaData.getPrimaryKeys(null, null, regionName.toUpperCase()))
          .thenReturn(primaryKeyResults);
      when(metaData.getTables(any(), any(), any(), any())).thenReturn(tableResults);

      preparedStatement = mock(PreparedStatement.class);
      when(preparedStatement.getUpdateCount()).thenReturn(1);

      connection = mock(Connection.class);
      when(connection.getMetaData()).thenReturn(metaData);
      when(connection.prepareStatement(any())).thenReturn(preparedStatement);

      return connection;
    }
  }

  @Before
  public void setUp() throws Exception {}

  @After
  public void tearDown() throws Exception {}

  private void createManager(String driver, String url) throws SQLException {
    ResultSet rsKeys = mock(ResultSet.class);
    when(rsKeys.next()).thenReturn(true, false);
    when(rsKeys.getString("COLUMN_NAME")).thenReturn(ID_COLUMN_NAME);

    ResultSet rs = mock(ResultSet.class);
    when(rs.next()).thenReturn(true, false);
    when(rs.getString("TABLE_NAME")).thenReturn(regionName.toUpperCase());

    this.mgr = new TestableJDBCManagerWithResultSets(createConfiguration(driver, url), rs, rsKeys);
  }

  private void createDefaultManager() throws SQLException {
    createManager("java.lang.String", "fakeURL");
  }

  private void createUpsertManager() {
    this.mgr = new TestableUpsertJDBCManager(createConfiguration("java.lang.String", "fakeURL"));
  }

  private void createUpsertManager(int upsertReturn) {
    this.mgr = new TestableUpsertJDBCManager(createConfiguration("java.lang.String", "fakeURL"),
        upsertReturn);
  }

  private void createManager(ResultSet tableNames, ResultSet primaryKeys) {
    this.mgr = new TestableJDBCManagerWithResultSets(
        createConfiguration("java.lang.String", "fakeURL"), tableNames, primaryKeys);
  }

  private JDBCConfiguration createConfiguration(String driver, String url) {
    Properties props = new Properties();
    props.setProperty("url", url);
    props.setProperty("driver", driver);
    return new JDBCConfiguration(props);
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
    ArgumentCaptor<Object> objectCaptor = ArgumentCaptor.forClass(Object.class);
    verify(this.preparedStatement, times(3)).setObject(anyInt(), objectCaptor.capture());
    List<Object> allObjects = objectCaptor.getAllValues();
    assertThat(allObjects.get(0)).isEqualTo("Emp1");
    assertThat(allObjects.get(1)).isEqualTo(21);
    assertThat(allObjects.get(2)).isEqualTo("1");
  }

  @Test
  public void verifySimpleCreateWithIdField() throws SQLException {
    createDefaultManager();
    GemFireCacheImpl cache = Fakes.cache();
    Region region = Fakes.region(regionName, cache);
    PdxInstanceImpl pdx1 = mockPdxInstance("Emp1", 21, true);
    this.mgr.write(region, Operation.CREATE, "1", pdx1);
    verify(this.preparedStatement).execute();
    ArgumentCaptor<String> sqlCaptor = ArgumentCaptor.forClass(String.class);
    verify(this.connection).prepareStatement(sqlCaptor.capture());
    assertThat(sqlCaptor.getValue()).isEqualTo("INSERT INTO " + regionName + "(" + NAME_COLUMN_NAME
        + ", " + AGE_COLUMN_NAME + ", " + ID_COLUMN_NAME + ") VALUES (?,?,?)");
    ArgumentCaptor<Object> objectCaptor = ArgumentCaptor.forClass(Object.class);
    verify(this.preparedStatement, times(3)).setObject(anyInt(), objectCaptor.capture());
    List<Object> allObjects = objectCaptor.getAllValues();
    assertThat(allObjects.get(0)).isEqualTo("Emp1");
    assertThat(allObjects.get(1)).isEqualTo(21);
    assertThat(allObjects.get(2)).isEqualTo("1");
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
    ArgumentCaptor<Object> objectCaptor = ArgumentCaptor.forClass(Object.class);
    verify(this.preparedStatement, times(3)).setObject(anyInt(), objectCaptor.capture());
    List<Object> allObjects = objectCaptor.getAllValues();
    assertThat(allObjects.get(0)).isEqualTo("Emp1");
    assertThat(allObjects.get(1)).isEqualTo(21);
    assertThat(allObjects.get(2)).isEqualTo("1");
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
    ArgumentCaptor<Object> objectCaptor = ArgumentCaptor.forClass(Object.class);
    verify(this.preparedStatement, times(1)).setObject(anyInt(), objectCaptor.capture());
    List<Object> allObjects = objectCaptor.getAllValues();
    assertThat(allObjects.get(0)).isEqualTo("1");
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
    ArgumentCaptor<Object> objectCaptor = ArgumentCaptor.forClass(Object.class);
    verify(this.preparedStatement, times(6)).setObject(anyInt(), objectCaptor.capture());
    List<Object> allObjects = objectCaptor.getAllValues();
    assertThat(allObjects.get(0)).isEqualTo("Emp1");
    assertThat(allObjects.get(1)).isEqualTo(21);
    assertThat(allObjects.get(2)).isEqualTo("1");
    assertThat(allObjects.get(3)).isEqualTo("Emp2");
    assertThat(allObjects.get(4)).isEqualTo(55);
    assertThat(allObjects.get(5)).isEqualTo("2");
  }

  private PdxInstanceImpl mockPdxInstance(String name, int age) {
    return mockPdxInstance(name, age, false);
  }

  private PdxInstanceImpl mockPdxInstance(String name, int age, boolean addId) {
    PdxInstanceImpl pdxInstance = mock(PdxInstanceImpl.class);
    if (addId) {
      when(pdxInstance.getFieldNames())
          .thenReturn(Arrays.asList(NAME_COLUMN_NAME, AGE_COLUMN_NAME, ID_COLUMN_NAME));
      when(pdxInstance.getField(ID_COLUMN_NAME)).thenReturn("bogusId");
    } else {
      when(pdxInstance.getFieldNames())
          .thenReturn(Arrays.asList(NAME_COLUMN_NAME, AGE_COLUMN_NAME));
    }
    when(pdxInstance.getField(NAME_COLUMN_NAME)).thenReturn(name);
    when(pdxInstance.getField(AGE_COLUMN_NAME)).thenReturn(age);
    PdxType pdxType = mock(PdxType.class);
    when(pdxType.getTypeId()).thenReturn(1);
    when(pdxInstance.getPdxType()).thenReturn(pdxType);
    return pdxInstance;
  }

  @Test
  public void verifyMissingDriverClass() throws SQLException {
    createManager("non existent driver", "fakeURL");
    catchException(this.mgr).getConnection(null, null);
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

  @Test
  public void verifyNoTableForRegion() throws SQLException {
    createDefaultManager();
    GemFireCacheImpl cache = Fakes.cache();
    Region region = Fakes.region("badRegion", cache);
    catchException(this.mgr).write(region, Operation.DESTROY, "1", null);
    assertThat((Exception) caughtException()).isInstanceOf(IllegalStateException.class);
    assertThat(caughtException().getMessage())
        .isEqualTo("no table was found that matches badRegion");
  }

  @Test
  public void verifyInsertUpdate() throws SQLException {
    createUpsertManager();
    GemFireCacheImpl cache = Fakes.cache();
    Region region = Fakes.region(regionName, cache);
    PdxInstanceImpl pdx1 = mockPdxInstance("Emp1", 21);
    this.mgr.write(region, Operation.CREATE, "1", pdx1);
    verify(this.preparedStatement).execute();
    verify(this.preparedStatement2).execute();
    ArgumentCaptor<String> sqlCaptor = ArgumentCaptor.forClass(String.class);
    verify(this.connection, times(2)).prepareStatement(sqlCaptor.capture());
    List<String> allArgs = sqlCaptor.getAllValues();
    assertThat(allArgs.get(0)).isEqualTo("INSERT INTO " + regionName + "(" + NAME_COLUMN_NAME + ", "
        + AGE_COLUMN_NAME + ", " + ID_COLUMN_NAME + ") VALUES (?,?,?)");
    assertThat(allArgs.get(1)).isEqualTo("UPDATE " + regionName + " SET " + NAME_COLUMN_NAME
        + " = ?, " + AGE_COLUMN_NAME + " = ? WHERE " + ID_COLUMN_NAME + " = ?");
    ArgumentCaptor<Object> objectCaptor = ArgumentCaptor.forClass(Object.class);
    verify(this.preparedStatement, times(3)).setObject(anyInt(), objectCaptor.capture());
    List<Object> allObjects = objectCaptor.getAllValues();
    assertThat(allObjects.get(0)).isEqualTo("Emp1");
    assertThat(allObjects.get(1)).isEqualTo(21);
    assertThat(allObjects.get(2)).isEqualTo("1");
    verify(this.preparedStatement2, times(3)).setObject(anyInt(), objectCaptor.capture());
    allObjects = objectCaptor.getAllValues();
    assertThat(allObjects.get(0)).isEqualTo("Emp1");
    assertThat(allObjects.get(1)).isEqualTo(21);
    assertThat(allObjects.get(2)).isEqualTo("1");
  }

  @Test
  public void verifyUpdateInsert() throws SQLException {
    createUpsertManager();
    GemFireCacheImpl cache = Fakes.cache();
    Region region = Fakes.region(regionName, cache);
    PdxInstanceImpl pdx1 = mockPdxInstance("Emp1", 21);
    this.mgr.write(region, Operation.UPDATE, "1", pdx1);
    verify(this.preparedStatement).execute();
    verify(this.preparedStatement2).execute();
    ArgumentCaptor<String> sqlCaptor = ArgumentCaptor.forClass(String.class);
    verify(this.connection, times(2)).prepareStatement(sqlCaptor.capture());
    List<String> allArgs = sqlCaptor.getAllValues();
    assertThat(allArgs.get(0)).isEqualTo("UPDATE " + regionName + " SET " + NAME_COLUMN_NAME
        + " = ?, " + AGE_COLUMN_NAME + " = ? WHERE " + ID_COLUMN_NAME + " = ?");
    assertThat(allArgs.get(1)).isEqualTo("INSERT INTO " + regionName + "(" + NAME_COLUMN_NAME + ", "
        + AGE_COLUMN_NAME + ", " + ID_COLUMN_NAME + ") VALUES (?,?,?)");
    ArgumentCaptor<Object> objectCaptor = ArgumentCaptor.forClass(Object.class);
    verify(this.preparedStatement, times(3)).setObject(anyInt(), objectCaptor.capture());
    List<Object> allObjects = objectCaptor.getAllValues();
    assertThat(allObjects.get(0)).isEqualTo("Emp1");
    assertThat(allObjects.get(1)).isEqualTo(21);
    assertThat(allObjects.get(2)).isEqualTo("1");
    verify(this.preparedStatement2, times(3)).setObject(anyInt(), objectCaptor.capture());
    allObjects = objectCaptor.getAllValues();
    assertThat(allObjects.get(0)).isEqualTo("Emp1");
    assertThat(allObjects.get(1)).isEqualTo(21);
    assertThat(allObjects.get(2)).isEqualTo("1");
  }

  @Test
  public void verifyInsertUpdateThatUpdatesNothing() throws SQLException {
    createUpsertManager(0);
    GemFireCacheImpl cache = Fakes.cache();
    Region region = Fakes.region(regionName, cache);
    PdxInstanceImpl pdx1 = mockPdxInstance("Emp1", 21);
    catchException(this.mgr).write(region, Operation.CREATE, "1", pdx1);
    assertThat((Exception) caughtException()).isInstanceOf(IllegalStateException.class);
    assertThat(caughtException().getMessage()).isEqualTo("Unexpected updateCount 0");
    verify(this.preparedStatement).execute();
    verify(this.preparedStatement2).execute();
    ArgumentCaptor<String> sqlCaptor = ArgumentCaptor.forClass(String.class);
    verify(this.connection, times(2)).prepareStatement(sqlCaptor.capture());
    List<String> allArgs = sqlCaptor.getAllValues();
    assertThat(allArgs.get(0)).isEqualTo("INSERT INTO " + regionName + "(" + NAME_COLUMN_NAME + ", "
        + AGE_COLUMN_NAME + ", " + ID_COLUMN_NAME + ") VALUES (?,?,?)");
    assertThat(allArgs.get(1)).isEqualTo("UPDATE " + regionName + " SET " + NAME_COLUMN_NAME
        + " = ?, " + AGE_COLUMN_NAME + " = ? WHERE " + ID_COLUMN_NAME + " = ?");
    ArgumentCaptor<Object> objectCaptor = ArgumentCaptor.forClass(Object.class);
    verify(this.preparedStatement, times(3)).setObject(anyInt(), objectCaptor.capture());
    List<Object> allObjects = objectCaptor.getAllValues();
    assertThat(allObjects.get(0)).isEqualTo("Emp1");
    assertThat(allObjects.get(1)).isEqualTo(21);
    assertThat(allObjects.get(2)).isEqualTo("1");
    verify(this.preparedStatement2, times(3)).setObject(anyInt(), objectCaptor.capture());
    allObjects = objectCaptor.getAllValues();
    assertThat(allObjects.get(0)).isEqualTo("Emp1");
    assertThat(allObjects.get(1)).isEqualTo(21);
    assertThat(allObjects.get(2)).isEqualTo("1");
  }

  @Test
  public void twoTablesOfSameName() throws SQLException {
    ResultSet primaryKeys = null;
    ResultSet tables = mock(ResultSet.class);
    when(tables.next()).thenReturn(true, true, false);
    when(tables.getString("TABLE_NAME")).thenReturn(regionName.toUpperCase());
    createManager(tables, primaryKeys);
    catchException(this.mgr).computeKeyColumnName(regionName);
    assertThat((Exception) caughtException()).isInstanceOf(IllegalStateException.class);
    assertThat(caughtException().getMessage()).isEqualTo("Duplicate tables that match region name");
  }

  @Test
  public void noPrimaryKeyOnTable() throws SQLException {
    ResultSet tables = null;
    ResultSet primaryKeys = mock(ResultSet.class);
    when(primaryKeys.next()).thenReturn(false);
    createManager(tables, primaryKeys);
    catchException(this.mgr).computeKeyColumnName(regionName);
    assertThat((Exception) caughtException()).isInstanceOf(IllegalStateException.class);
    assertThat(caughtException().getMessage())
        .isEqualTo("The table " + regionName + " does not have a primary key column.");
  }

  @Test
  public void twoPrimaryKeysOnTable() throws SQLException {
    ResultSet tables = null;
    ResultSet primaryKeys = mock(ResultSet.class);
    when(primaryKeys.next()).thenReturn(true, true, false);
    createManager(tables, primaryKeys);
    catchException(this.mgr).computeKeyColumnName(regionName);
    assertThat((Exception) caughtException()).isInstanceOf(IllegalStateException.class);
    assertThat(caughtException().getMessage())
        .isEqualTo("The table " + regionName + " has more than one primary key column.");
  }
}
