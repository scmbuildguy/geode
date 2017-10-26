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
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.geode.cache.Operation;
import org.apache.geode.cache.Region;
import org.apache.geode.pdx.PdxInstance;

public class JDBCManager {

  private final JDBCConfiguration config;

  private Connection conn;

  JDBCManager(JDBCConfiguration config) {
    this.config = config;
  }

  public class ColumnValue {

    final private boolean isKey;
    final private String columnName;
    final private Object value;

    public ColumnValue(boolean isKey, String columnName, Object value) {
      this.isKey = isKey;
      this.columnName = columnName;
      this.value = value;
    }

    public boolean isKey() {
      return this.isKey;
    }

    public String getColumnName() {
      return this.columnName;
    }

    public Object getValue() {
      return this.value;
    }
  }

  public void write(Region region, Operation operation, Object key, PdxInstance value) {
    String tableName = getTableName(region);
    List<ColumnValue> columnList = getColumnToValueList(tableName, key, value, operation);
    PreparedStatement pstmt = getQueryStatement(columnList, tableName, operation);
    try {
      int idx = 0;
      for (ColumnValue cv : columnList) {
        idx++;
        pstmt.setObject(idx, cv.getValue());
      }
      pstmt.execute();
    } catch (SQLException e) {
      handleSQLException(e);
    } finally {
      clearStatement(pstmt);
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

  private Connection getConnection() {
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
        throw new IllegalStateException("Driver class " + this.config.getDriver() + " not found",
            e);
      }
    }
    try {
      result = DriverManager.getConnection(this.config.getURL());
    } catch (SQLException e) {
      // TODO: consider a different exception
      throw new IllegalStateException("Could not connect to " + this.config.getURL(), e);
    }
    this.conn = result;
    return result;
  }

  // private final ConcurrentMap<String, PreparedStatement> preparedStatementCache = new
  // ConcurrentHashMap<>();

  private PreparedStatement getQueryStatement(List<ColumnValue> columnList, String tableName,
      Operation operation) {
    // ConcurrentMap<String, PreparedStatement> cache = getPreparedStatementCache(operation);
    // return cache.computeIfAbsent(query, k -> {
    // String query = getQueryString(tableName, columnList, operation);
    // Connection con = getConnection();
    // try {
    // return con.prepareStatement(k);
    // } catch (SQLException e) {
    // handleSQLException(e);
    // }
    // });
    String query = getQueryString(tableName, columnList, operation);
    System.out.println("query=" + query);
    Connection con = getConnection();
    try {
      return con.prepareStatement(query);
    } catch (SQLException e) {
      handleSQLException(e);
      return null; // this line is never reached
    }
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
    // TODO Auto-generated method stub
    return false;
  }

  private String mapFieldNameToColumnName(String fieldName, String tableName) {
    // TODO check config for mapping
    return fieldName;
  }

  private final ConcurrentMap<String, String> tableToPrimaryKeyMap = new ConcurrentHashMap<>();

  private String getKeyColumnName(String tableName) {
    return tableToPrimaryKeyMap.computeIfAbsent(tableName, k -> {
      // TODO: check config for key column
      Connection con = getConnection();
      try {
        DatabaseMetaData metaData = con.getMetaData();
        ResultSet tablesRS = metaData.getTables(null, null, "%", null);
        String realTableName = null;
        while (tablesRS.next()) {
          String name = tablesRS.getString("TABLE_NAME");
          if (name.equalsIgnoreCase(k)) {
            if (realTableName != null) {
              throw new IllegalStateException("Duplicate tables that match region name");
            }
            realTableName = name;
          }
        }
        if (realTableName == null) {
          throw new IllegalStateException("no table was found that matches " + k);
        }
        ResultSet primaryKeys = metaData.getPrimaryKeys(null, null, realTableName);
        if (!primaryKeys.next()) {
          throw new IllegalStateException(
              "The table " + k + " does not have a primary key column.");
        }
        String key = primaryKeys.getString("COLUMN_NAME");
        if (primaryKeys.next()) {
          throw new IllegalStateException(
              "The table " + k + " has more than one primary key column.");
        }
        return key;
      } catch (SQLException e) {
        handleSQLException(e);
        return null; // never reached
      }
    });
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
