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

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.geode.cache.Operation;
import org.apache.geode.cache.Region;
import org.apache.geode.pdx.PdxInstance;
import org.apache.geode.pdx.internal.PdxInstanceImpl;

public class JDBCManager {

  private final JDBCConfiguration config;

  private Connection conn;

  private final ConcurrentMap<String, String> tableToPrimaryKeyMap = new ConcurrentHashMap<>();

  private final ThreadLocal<Map<StatementKey, PreparedStatement>> preparedStatementCache =
      new ThreadLocal<>();

  private Map<StatementKey, PreparedStatement> getPreparedStatementCache() {
    Map<StatementKey, PreparedStatement> result = preparedStatementCache.get();
    if (result == null) {
      result = new HashMap<>();
      preparedStatementCache.set(result);
    }
    return result;
  }

  JDBCManager(JDBCConfiguration config) {
    this.config = config;
  }

  public void write(Region region, Operation operation, Object key, PdxInstance value) {
    String tableName = getTableName(region);
    int pdxTypeId = 0;
    if (value != null) {
      pdxTypeId = ((PdxInstanceImpl) value).getPdxType().getTypeId();
    }
    List<ColumnValue> columnList = getColumnToValueList(tableName, key, value, operation);
    int updateCount = executeWrite(columnList, tableName, operation, pdxTypeId, false);
    if (operation.isDestroy()) {
      // TODO: should we check updateCount here? Probably not. It is possible we have nothing in the
      // table to destroy.
      return;
    }
    if (updateCount <= 0) {
      Operation upsertOp;
      if (operation.isUpdate()) {
        upsertOp = Operation.CREATE;
      } else {
        upsertOp = Operation.UPDATE;
      }
      updateCount = executeWrite(columnList, tableName, upsertOp, pdxTypeId, true);
    }
    if (updateCount != 1) {
      throw new IllegalStateException("Unexpected updateCount " + updateCount);
    }
  }

  private int executeWrite(List<ColumnValue> columnList, String tableName, Operation operation,
      int pdxTypeId, boolean handleException) {
    PreparedStatement pstmt = getPreparedStatement(columnList, tableName, operation, pdxTypeId);
    synchronized (pstmt) {
      try {
        int idx = 0;
        for (ColumnValue cv : columnList) {
          idx++;
          pstmt.setObject(idx, cv.getValue());
        }
        pstmt.execute();
        return pstmt.getUpdateCount();
      } catch (SQLException e) {
        if (handleException || operation.isDestroy()) {
          handleSQLException(e);
        }
        return 0;
      } finally {
        clearStatement(pstmt);
      }
    }
  }

  private void clearStatement(PreparedStatement ps) {
    try {
      ps.clearParameters();
    } catch (SQLException ignore) {
    }
  }

  private String getQueryString(String tableName, List<ColumnValue> columnList,
      Operation operation) {
    if (operation.isCreate()) {
      return getInsertQueryString(tableName, columnList);
    } else if (operation.isUpdate()) {
      return getUpdateQueryString(tableName, columnList);
    } else if (operation.isDestroy()) {
      return getDestroyQueryString(tableName, columnList);
    } else {
      throw new IllegalStateException("unsupported operation " + operation);
    }
  }

  private String getDestroyQueryString(String tableName, List<ColumnValue> columnList) {
    assert columnList.size() == 1;
    ColumnValue keyCV = columnList.get(0);
    assert keyCV.isKey();
    StringBuilder query =
        new StringBuilder("DELETE FROM " + tableName + " WHERE " + keyCV.getColumnName() + " = ?");
    return query.toString();
  }

  private String getUpdateQueryString(String tableName, List<ColumnValue> columnList) {
    StringBuilder query = new StringBuilder("UPDATE " + tableName + " SET ");
    int idx = 0;
    for (ColumnValue cv : columnList) {
      if (cv.isKey()) {
        query.append(" WHERE ");
      } else {
        idx++;
        if (idx > 1) {
          query.append(", ");
        }
      }
      query.append(cv.getColumnName());
      query.append(" = ?");
    }
    return query.toString();
  }

  private String getInsertQueryString(String tableName, List<ColumnValue> columnList) {
    StringBuilder columnNames = new StringBuilder("INSERT INTO " + tableName + '(');
    StringBuilder columnValues = new StringBuilder(" VALUES (");
    int columnCount = columnList.size();
    int idx = 0;
    for (ColumnValue cv : columnList) {
      idx++;
      columnNames.append(cv.getColumnName());
      columnValues.append('?');
      if (idx != columnCount) {
        columnNames.append(", ");
        columnValues.append(",");
      }
    }
    columnNames.append(")");
    columnValues.append(")");
    return columnNames.append(columnValues).toString();
  }

  Connection getConnection(String user, String password) {
    Connection result = this.conn;
    try {
      if (result != null && !result.isClosed()) {
        return result;
      }
    } catch (SQLException ignore) {
      // If isClosed throws fall through and connect again
    }

    if (result == null) {
      try {
        Class.forName(this.config.getDriver());
      } catch (ClassNotFoundException e) {
        // TODO: consider a different exception
        throw new IllegalStateException(
            "Driver class \"" + this.config.getDriver() + "\" not found", e);
      }
    }
    try {
      result =
          createConnection(this.config.getURL(), this.config.getUser(), this.config.getPassword());
    } catch (SQLException e) {
      // TODO: consider a different exception
      throw new IllegalStateException("Could not connect to " + this.config.getURL(), e);
    }
    this.conn = result;
    return result;
  }

  protected Connection createConnection(String url, String user, String password)
      throws SQLException {
    return DriverManager.getConnection(url);
  }

  private static class StatementKey {
    private final int pdxTypeId;
    private final Operation operation;
    private final String tableName;

    public StatementKey(int pdxTypeId, Operation operation, String tableName) {
      this.pdxTypeId = pdxTypeId;
      this.operation = operation;
      this.tableName = tableName;
    }

    @Override
    public int hashCode() {
      return operation.hashCode() + pdxTypeId + tableName.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      }
      if (obj == null) {
        return false;
      }
      StatementKey other = (StatementKey) obj;
      if (!operation.equals(other.operation)) {
        return false;
      }
      if (pdxTypeId != other.pdxTypeId) {
        return false;
      }
      if (!tableName.equals(other.tableName)) {
        return false;
      }
      return true;
    }
  }

  private PreparedStatement getPreparedStatement(List<ColumnValue> columnList, String tableName,
      Operation operation, int pdxTypeId) {
    System.out.println("getPreparedStatement : " + pdxTypeId + "operation: " + operation
        + " columns: " + columnList);
    StatementKey key = new StatementKey(pdxTypeId, operation, tableName);
    return getPreparedStatementCache().computeIfAbsent(key, k -> {
      String query = getQueryString(tableName, columnList, operation);
      System.out.println("query=" + query);
      Connection con = getConnection(null, null);
      try {
        return con.prepareStatement(query);
      } catch (SQLException e) {
        handleSQLException(e);
        return null; // this line is never reached
      }
    });
  }

  private List<ColumnValue> getColumnToValueList(String tableName, Object key, PdxInstance value,
      Operation operation) {
    String keyColumnName = getKeyColumnName(tableName);
    ColumnValue keyCV = new ColumnValue(true, keyColumnName, key);
    if (operation.isDestroy()) {
      return Collections.singletonList(keyCV);
    }

    List<String> fieldNames = value.getFieldNames();
    List<ColumnValue> result = new ArrayList<>(fieldNames.size() + 1);
    for (String fieldName : fieldNames) {
      if (isFieldExcluded(fieldName)) {
        continue;
      }
      String columnName = mapFieldNameToColumnName(fieldName, tableName);
      if (columnName.equalsIgnoreCase(keyColumnName)) {
        continue;
      }
      Object columnValue = value.getField(fieldName);
      ColumnValue cv = new ColumnValue(false, fieldName, columnValue);
      // TODO: any need to order the items in the list?
      result.add(cv);
    }
    result.add(keyCV);
    return result;
  }

  private boolean isFieldExcluded(String fieldName) {
    // TODO check configuration
    return false;
  }

  private String mapFieldNameToColumnName(String fieldName, String tableName) {
    // TODO check config for mapping
    return fieldName;
  }

  private String getKeyColumnName(String tableName) {
    return tableToPrimaryKeyMap.computeIfAbsent(tableName, k -> {
      return computeKeyColumnName(k);
    });
  }

  String computeKeyColumnName(String tableName) {
    // TODO: check config for key column
    Connection con = getConnection(null, null);
    try {
      DatabaseMetaData metaData = con.getMetaData();
      ResultSet tablesRS = metaData.getTables(null, null, "%", null);
      String realTableName = null;
      while (tablesRS.next()) {
        String name = tablesRS.getString("TABLE_NAME");
        if (name.equalsIgnoreCase(tableName)) {
          if (realTableName != null) {
            throw new IllegalStateException("Duplicate tables that match region name");
          }
          realTableName = name;
        }
      }
      if (realTableName == null) {
        throw new IllegalStateException("no table was found that matches " + tableName);
      }
      ResultSet primaryKeys = metaData.getPrimaryKeys(null, null, realTableName);
      if (!primaryKeys.next()) {
        throw new IllegalStateException(
            "The table " + tableName + " does not have a primary key column.");
      }
      String key = primaryKeys.getString("COLUMN_NAME");
      if (primaryKeys.next()) {
        throw new IllegalStateException(
            "The table " + tableName + " has more than one primary key column.");
      }
      return key;
    } catch (SQLException e) {
      handleSQLException(e);
      return null; // never reached
    }
  }

  private void handleSQLException(SQLException e) {
    throw new IllegalStateException("NYI: handleSQLException", e);
  }

  private String getTableName(Region region) {
    // TODO: check config for mapping
    return region.getName();
  }

  private void printResultSet(ResultSet rs) {
    System.out.println("Printing ResultSet:");
    try {
      int size = 0;
      ResultSetMetaData rsmd = rs.getMetaData();
      int columnsNumber = rsmd.getColumnCount();
      while (rs.next()) {
        size++;
        for (int i = 1; i <= columnsNumber; i++) {
          if (i > 1)
            System.out.print(",  ");
          String columnValue = rs.getString(i);
          System.out.print(rsmd.getColumnName(i) + ": " + columnValue);
        }
        System.out.println("");
      }
      System.out.println("size=" + size);
    } catch (SQLException ex) {
      System.out.println("Exception while printing result set" + ex);
    } finally {
      try {
        rs.beforeFirst();
      } catch (SQLException e) {
        System.out.println("Exception while calling beforeFirst" + e);
      }
    }
  }
}
