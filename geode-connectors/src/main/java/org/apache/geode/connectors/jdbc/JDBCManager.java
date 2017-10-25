package org.apache.geode.connectors.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.geode.cache.Operation;
import org.apache.geode.cache.Region;
import org.apache.geode.pdx.PdxInstance;

public class JDBCManager {

  private final JDBCConfiguration config;

  private Connection conn;

  private Statement stmt;

  JDBCManager(JDBCConfiguration config) {
    this.config = config;
  }

  private void establishConnection() {
    // Class.forName(this.config.getDriver());
    // conn = DriverManager.getConnection(this.config.getURL());
    // stmt = conn.createStatement();
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
    String query = getQueryString(tableName, columnList, operation);
    Statement statement = getQueryStatement(columnList, query);
    try {
      statement.execute(query);
    } catch (SQLException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
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
    // TODO Auto-generated method stub
    return null;
  }

  private String getUpdateQueryString(String tableName, List<ColumnValue> columnList) {
    // TODO Auto-generated method stub
    return null;
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

  private Statement getQueryStatement(List<ColumnValue> columnList, String query) {

    // TODO Auto-generated method stub
    return null;
  }

  private List<ColumnValue> getColumnToValueList(String tableName, Object key, PdxInstance value, Operation operation) {
    Set<String> keyColumnNames = getKeyColumnNames(tableName);
    for (String fieldName: value.getFieldNames()) {
      String columnName = mapFieldNameToColumnName(fieldName, tableName);
      boolean isKey = keyColumnNames.contains(columnName);
      
      if (operation.isDestroy() && !isKey) {
        continue;
      }
      // TODO: what if isKey and value needs to be the key object?
      Object columnValue = value.getField(fieldName);
      ColumnValue cv = new ColumnValue(isKey, fieldName, columnValue);
    }
    String[] valueColumnNames = getValueColumnNames(tableName, value);
    return null;
  }

  private String mapFieldNameToColumnName(String fieldName, String tableName) {
    // TODO check config for mapping
    return fieldName;
  }

  private Set<String> getKeyColumnNames(String tableName) {
    // TODO Auto-generated method stub
    return null;
  }

  private String getTableName(Region region) {
    // TODO: check config for mapping
    return region.getName();
  }



}
