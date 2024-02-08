/*
 * Copyright 2017 YCSB Contributors. All Rights Reserved.
 *
 * CODE IS BASED ON the jdbc-binding JdbcDBClient class.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */
package site.ycsb.postgrenosql;

import site.ycsb.*;
import org.json.simple.JSONObject;
import org.postgresql.Driver;
import org.postgresql.util.PGobject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

/**
 * PostgreNoSQL client for YCSB framework.
 */
public class PostgreNoSQLDBClient extends DB {
  private static final Logger LOG = LoggerFactory.getLogger(PostgreNoSQLDBClient.class);

  /** Count the number of times initialized to teardown on the last. */
  private static final AtomicInteger INIT_COUNT = new AtomicInteger(0);

  /** Cache for already prepared statements. */
  private static ConcurrentMap<StatementType, PreparedStatement> cachedStatements;

  /** The driver to get the connection to postgresql. */
  private static Driver postgrenosqlDriver;

  /** The connection to the database. */
  private static Connection connection;

  /** The class to use as the jdbc driver. */
  public static final String DRIVER_CLASS = "db.driver";

  /** The URL to connect to the database. */
  public static final String CONNECTION_URL = "postgrenosql.url";

  /** The user name to use to connect to the database. */
  public static final String CONNECTION_USER = "postgrenosql.user";

  /** The password to use for establishing the connection. */
  public static final String CONNECTION_PASSWD = "postgrenosql.passwd";

  /** The JDBC connection auto-commit property for the driver. */
  public static final String JDBC_AUTO_COMMIT = "postgrenosql.autocommit";

  /** The primary key in the user table. */
  public static final String PRIMARY_KEY = "YCSB_KEY";

  /** The element entity in the document. */
  public static final String BUSINESS_ID = "field9";

  /** The field name prefix in the table. */
  public static final String COLUMN_NAME = "YCSB_VALUE";

  private static final String DEFAULT_PROP = "";

  /** Returns parsed boolean value from the properties if set, otherwise returns defaultVal. */
  private static boolean getBoolProperty(Properties props, String key, boolean defaultVal) {
    String valueStr = props.getProperty(key);
    if (valueStr != null) {
      return Boolean.parseBoolean(valueStr);
    }
    return defaultVal;
  }

  @Override
  public void init() throws DBException {
    INIT_COUNT.incrementAndGet();
    synchronized (PostgreNoSQLDBClient.class) {
      if (postgrenosqlDriver != null) {
        return;
      }

      Properties props = getProperties();
      String urls = props.getProperty(CONNECTION_URL, DEFAULT_PROP);
      String user = props.getProperty(CONNECTION_USER, DEFAULT_PROP);
      String passwd = props.getProperty(CONNECTION_PASSWD, DEFAULT_PROP);
      boolean autoCommit = getBoolProperty(props, JDBC_AUTO_COMMIT, true);

      try {
        Properties tmpProps = new Properties();
        tmpProps.setProperty("user", user);
        tmpProps.setProperty("password", passwd);

        cachedStatements = new ConcurrentHashMap<>();

        postgrenosqlDriver = new Driver();
        connection = postgrenosqlDriver.connect(urls, tmpProps);
        connection.setAutoCommit(autoCommit);

      } catch (Exception e) {
        LOG.error("Error during initialization: " + e);
      }
    }
  }

  @Override
  public void cleanup() throws DBException {
    if (INIT_COUNT.decrementAndGet() == 0) {
      try {
        cachedStatements.clear();

        if (!connection.getAutoCommit()){
          connection.commit();
        }
        connection.close();
      } catch (SQLException e) {
        System.err.println("Error in cleanup execution. " + e);
      }
      postgrenosqlDriver = null;
    }
  }

  @Override
  public Status read(String tableName, String entity, Set<String> fields, Map<String, ByteIterator> result) {
    StatementType type = new StatementType(StatementType.Type.READ, tableName, fields);
    PreparedStatement readStatement = cachedStatements.get(type);
    try {
      if (readStatement == null) {
        readStatement = createAndCacheReadStatement(type);
      }
      readStatement.setString(1, entity);
      ResultSet resultSet = readStatement.executeQuery();
      if (!resultSet.next()) {
        resultSet.close();
        return  Status.NOT_FOUND;
      }

      if (result != null) {
        if (fields == null){
          do{
            String field = resultSet.getString(2);
            String value = resultSet.getString(3);
            result.put(field, new StringByteIterator(value));
          }while (resultSet.next());
        } else {
          for (String field : fields) {
            String value = resultSet.getString(field);
            result.put(field, new StringByteIterator(value));
          }
        }
      }
      resultSet.close();
      LOG.info("read: {}", entity);
      return Status.OK;

    } catch (SQLException e) {
      LOG.error("Error in processing read of table " + tableName + ": " + e);
      LOG.error(readStatement.toString());
      return Status.ERROR;
    }
  }

  @Override
  public Status scan(String tableName, String startKey, int recordcount, Set<String> fields,
                     Vector<HashMap<String, ByteIterator>> result) {
    StatementType type = new StatementType(StatementType.Type.SCAN, tableName, fields);
    PreparedStatement scanStatement = cachedStatements.get(type);
    try {
//      StatementType type = new StatementType(StatementType.Type.SCAN, tableName, fields);
//      PreparedStatement scanStatement = cachedStatements.get(type);
      if (scanStatement == null) {
        scanStatement = createAndCacheScanStatement(type);
      }
//      scanStatement.setString(1, startKey);
      scanStatement.setFloat(1, 4);
      scanStatement.setInt(2, recordcount);
      ResultSet resultSet = scanStatement.executeQuery();
      for (int i = 0; i < recordcount && resultSet.next(); i++) {
        if (result != null && fields != null) {
          HashMap<String, ByteIterator> values = new HashMap<String, ByteIterator>();
          for (String field : fields) {
            String value = resultSet.getString(field);
            values.put(field, new StringByteIterator(value));
          }

          result.add(values);
        }
      }

      resultSet.close();
      return Status.OK;
    } catch (SQLException e) {
      LOG.error("Error in processing scan of table: " + tableName + ": " + e);
      return Status.ERROR;
    }
  }

  @Override
  public Status update(String tableName, String key, Map<String, ByteIterator> values) {
    StatementType type = new StatementType(StatementType.Type.UPDATE, tableName, null);
    PreparedStatement updateStatement = cachedStatements.get(type);
    try{
//      StatementType type = new StatementType(StatementType.Type.UPDATE, tableName, null);
//      PreparedStatement updateStatement = cachedStatements.get(type);
      if (updateStatement == null) {
        updateStatement = createAndCacheUpdateStatement(type);
      }

      JSONObject jsonObject = new JSONObject();

      //minimum rate value of 5 has been set to differ original value from updated one
      jsonObject.put("stars", Float.parseFloat(buildRandomRate(5)));

      PGobject object = new PGobject();
      object.setType("jsonb");
      object.setValue(jsonObject.toJSONString());

      updateStatement.setObject(1, object);
      updateStatement.setString(2, key);

      int result = updateStatement.executeUpdate();
      if (result == 1) {
        LOG.info("updated: {}", key);
        return Status.OK;
      }
      return Status.UNEXPECTED_STATE;
    } catch (SQLException e) {
      LOG.error("Error in processing update to table: " + tableName + e);
      LOG.error(updateStatement.toString());
      return Status.ERROR;
    }
  }


  /**
   * Build a rate field.
   */
  private String buildRandomRate(int minRate) {
    String rateInString;
    int size = 31;
    float [] rates = new float[size];
    for (int i = 0; i < size; i++) {
      rates[i] = minRate + (float)i/10;
    }
    int randomElement = (int)(Math.random() * size);
    rateInString = Float.toString(rates[randomElement]);
    return rateInString;
  }


  @Override
  public Status insert(String tableName, String key, Map<String, ByteIterator> values) {
    StatementType type = new StatementType(StatementType.Type.INSERT, tableName, null);
    PreparedStatement insertStatement = cachedStatements.get(type);
    try{
//      StatementType type = new StatementType(StatementType.Type.INSERT, tableName, null);
//      PreparedStatement insertStatement = cachedStatements.get(type);
      if (insertStatement == null) {
        insertStatement = createAndCacheInsertStatement(type);
      }

      JSONObject jsonObject = new JSONObject();

      for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
        if (entry.getKey().equals("field0")) {

          jsonObject.put("full_address", entry.getValue().toString());

        } else if(entry.getKey().equals("field1")) {

          JSONObject hours = getHours();

          jsonObject.put("hours", hours);

        } else if(entry.getKey().equals("field2")) {
          jsonObject.put("city", "Phoenix");

        } else if(entry.getKey().equals("field3")) {
          jsonObject.put("review_count", 4);

        }  else if(entry.getKey().equals("field4")) {
          jsonObject.put("longitude", Double.parseDouble(entry.getValue().toString()));

        }  else if(entry.getKey().equals("field5")) {
          jsonObject.put("latitude", Double.parseDouble(entry.getValue().toString()));

        } else if(entry.getKey().equals("field8")) {
          jsonObject.put("stars", Float.parseFloat(entry.getValue().toString()));

        } else if(entry.getKey().equals("field9")) {
          jsonObject.put("field9", entry.getValue().toString());
//          jsonObject.put("field9", entry.getValue());

        } else {
          jsonObject.put(entry.getKey(), entry.getValue().toString());
        }
        JSONObject attributes = getAttributes();
        jsonObject.put("attributes", attributes);

      }


      PGobject object = new PGobject();
      object.setType("jsonb");
      object.setValue(jsonObject.toJSONString());

      insertStatement.setObject(2, object);
      insertStatement.setString(1, key);

      int result = insertStatement.executeUpdate();
      if (result == 1) {
        LOG.info("inserted: {}", key);
        return Status.OK;
      }

      return Status.UNEXPECTED_STATE;
    } catch (SQLException e) {
      LOG.error("Error in processing insert to table: " + tableName + ": " + e);
      LOG.error(insertStatement.toString());
      return Status.ERROR;
    }
  }

  private static JSONObject getHours() {
    JSONObject hours = new JSONObject();

    JSONObject monday = new JSONObject();
    monday.put("open", "18:00");
    monday.put("close", "20:00");
    JSONObject tuesday = new JSONObject();
    tuesday.put("open", "13:00");
    tuesday.put("close", "20:00");
    JSONObject wednesday = new JSONObject();
    wednesday.put("open", "8:00");
    wednesday.put("close", "15:00");
    JSONObject thursday = new JSONObject();
    thursday.put("open", "10:00");
    thursday.put("close", "22:00");
    JSONObject friday = new JSONObject();
    friday.put("open", "10:00");
    friday.put("close", "22:00");
    JSONObject saturday = new JSONObject();
    saturday.put("open", "10:00");
    saturday.put("close", "22:00");
    JSONObject sunday = new JSONObject();
    sunday.put("open", "10:00");
    sunday.put("close", "23:00");

    hours.put("monday", monday);
    hours.put("tuesday", tuesday);
    hours.put("wednesday", wednesday);
    hours.put("thursday", thursday);
    hours.put("friday", friday);
    hours.put("saturday", saturday);
    hours.put("sunday", saturday);
    return hours;
  }

  private static JSONObject getAttributes() {
    Random randomBoolean = new Random();
    JSONObject parking = new JSONObject();
    parking.put("garage", randomBoolean.nextBoolean());
    parking.put("street", randomBoolean.nextBoolean());
    parking.put("validated", randomBoolean.nextBoolean());
    parking.put("lot", randomBoolean.nextBoolean());
    parking.put("valet", randomBoolean.nextBoolean());
    JSONObject attributes = new JSONObject();
    attributes.put("Parking", parking);
    attributes.put("Price range", 1);
    return attributes;
  }

  @Override
  public Status delete(String tableName, String key) {
    try{
      StatementType type = new StatementType(StatementType.Type.DELETE, tableName, null);
      PreparedStatement deleteStatement = cachedStatements.get(type);
      if (deleteStatement == null) {
        deleteStatement = createAndCacheDeleteStatement(type);
      }
      deleteStatement.setString(1, key);

      int result = deleteStatement.executeUpdate();
      if (result == 1){
        return Status.OK;
      }

      return Status.UNEXPECTED_STATE;
    } catch (SQLException e) {
      LOG.error("Error in processing delete to table: " + tableName + e);
      return Status.ERROR;
    }
  }

  private PreparedStatement createAndCacheReadStatement(StatementType readType)
      throws SQLException{
//    PreparedStatement readStatement = connection.prepareStatement(createReadStatement(readType));
//    PreparedStatement statement = cachedStatements.putIfAbsent(readType, readStatement);
//    if (statement == null) {
//      return readStatement;
//    }
//    return statement;
    return connection.prepareStatement(createReadStatement(readType));
  }

  private String createReadStatement(StatementType readType){
    StringBuilder read = new StringBuilder("SELECT " + PRIMARY_KEY + " AS " + PRIMARY_KEY);

    if (readType.getFields() == null) {
      read.append(", (jsonb_each_text(" + COLUMN_NAME + ")).*");
    } else {
      for (String field:readType.getFields()){
        read.append(", " + COLUMN_NAME + "->>'" + field + "' AS " + field);
      }
    }

    read.append(" FROM " + readType.getTableName());
    read.append(" WHERE ");
    read.append(COLUMN_NAME);
    read.append("->>");
    read.append("'");
    read.append(BUSINESS_ID);
    read.append("'");
    read.append(" = ");
    read.append("?");
    return read.toString();
  }



  /*
  private String createReadStatement(StatementType readType){
    StringBuilder read = new StringBuilder("SELECT " + PRIMARY_KEY + " AS " + PRIMARY_KEY);

    if (readType.getFields() == null) {
      read.append(", (jsonb_each_text(" + COLUMN_NAME + ")).*");
    } else {
      for (String field:readType.getFields()){
        read.append(", " + COLUMN_NAME + "->>'" + field + "' AS " + field);
      }
    }

    read.append(" FROM " + readType.getTableName());
    read.append(" WHERE ");
    read.append(PRIMARY_KEY);
    read.append(" = ");
    read.append("?");
    return read.toString();
  }
*/


  private PreparedStatement createAndCacheScanStatement(StatementType scanType)
      throws SQLException{
//    PreparedStatement scanStatement = connection.prepareStatement(createScanStatement(scanType));
//    PreparedStatement statement = cachedStatements.putIfAbsent(scanType, scanStatement);
//    if (statement == null) {
//      return scanStatement;
//    }
    return connection.prepareStatement(createScanStatement(scanType));
  }

  private String createScanStatement(StatementType scanType){
    StringBuilder scan = new StringBuilder("SELECT " + PRIMARY_KEY + " AS " + PRIMARY_KEY);
    if (scanType.getFields() != null){
      for (String field:scanType.getFields()){
        scan.append(", " + COLUMN_NAME + "->>'" + field + "' AS " + field);
      }
    }
    scan.append(" FROM " + scanType.getTableName());
    scan.append(" WHERE ");
//    scan.append(PRIMARY_KEY);
    scan.append("(ycsb_value->'stars')::float");
    scan.append(" >= ?");
    scan.append(" ORDER BY ");
    scan.append(PRIMARY_KEY);
    scan.append(" LIMIT ?");

    return scan.toString();
  }

  public PreparedStatement createAndCacheUpdateStatement(StatementType updateType)
      throws SQLException{
//    PreparedStatement updateStatement = connection.prepareStatement(createUpdateStatement(updateType));
//    PreparedStatement statement = cachedStatements.putIfAbsent(updateType, updateStatement);
//    if (statement == null) {
//      return updateStatement;
//    }
//    return statement;
    return connection.prepareStatement(createUpdateStatement(updateType));
  }

  private String createUpdateStatement(StatementType updateType){
    StringBuilder update = new StringBuilder("UPDATE ");
    update.append(updateType.getTableName());
    update.append(" SET ");
    update.append(COLUMN_NAME + " = " + COLUMN_NAME);
    update.append(" || ? ");
    update.append(" WHERE ");
    update.append(PRIMARY_KEY);
    update.append(" = ?");
    return update.toString();
  }

  private PreparedStatement createAndCacheInsertStatement(StatementType insertType)
      throws SQLException{
//    PreparedStatement insertStatement = connection.prepareStatement(createInsertStatement(insertType));
//    PreparedStatement statement = cachedStatements.putIfAbsent(insertType, insertStatement);
//    if (statement == null) {
//      return insertStatement;
//    }
//    return statement;
    return connection.prepareStatement(createInsertStatement(insertType));
  }

  private String createInsertStatement(StatementType insertType){
    StringBuilder insert = new StringBuilder("INSERT INTO ");
    insert.append(insertType.getTableName());
    insert.append(" (" + PRIMARY_KEY + "," + COLUMN_NAME + ")");
    insert.append(" VALUES(?,?)");
    return insert.toString();
  }

  private PreparedStatement createAndCacheDeleteStatement(StatementType deleteType)
      throws SQLException{
    PreparedStatement deleteStatement = connection.prepareStatement(createDeleteStatement(deleteType));
    PreparedStatement statement = cachedStatements.putIfAbsent(deleteType, deleteStatement);
    if (statement == null) {
      return deleteStatement;
    }
    return statement;
  }

  private String createDeleteStatement(StatementType deleteType){
    StringBuilder delete = new StringBuilder("DELETE FROM ");
    delete.append(deleteType.getTableName());
    delete.append(" WHERE ");
    delete.append(PRIMARY_KEY);
    delete.append(" = ?");
    return delete.toString();
  }
}
