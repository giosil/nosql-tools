package org.dew.nosql;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.net.NetworkInterface;
import java.nio.ByteBuffer;
import java.sql.Blob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.sql.Timestamp;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import javax.naming.Context;
import javax.naming.InitialContext;

import javax.sql.DataSource;

import org.dew.nosql.json.JSON;
import org.dew.nosql.util.WUtil;

public 
class NoSQLJdbc implements INoSQLDB 
{
  protected static String logprefix = NoSQLJdbc.class.getSimpleName() + ".";
  
  protected Connection _conn;
  protected String jdbcPath = "jdbc/";
  protected String dataSource;
  
  public static Boolean _useSequece    = null;
  public static Boolean _numSequence   = Boolean.TRUE;
  public static String _dbms           = "sql";
  public static Object _trueValue      = new Integer(1);
  public static Object _falseValue     = new Integer(0);
  public static String _idFieldName    = "ID";
  public static String _tablesPrefix   = "NOS_";
  public static String _sequenceTab    = _tablesPrefix + "SEQUENCES";
  public static String _sequenceKey    = "NAME";
  public static String _sequenceVal    = "VALUE";
  public static String _sequencePrefix = "SEQ_";
  public static String _sequenceSuffix = "";
  public static String _filesTable     = _tablesPrefix + "FS_FILES";
  public static String _filesNameField = "NAME";
  public static String _filesContField = "CONTENT";
  public static String _filesEmptyFunc = "EMPTY_BLOB()";
  public static int    _queryTimeout   = 2 * 60;
  
  protected boolean debug = false;
  protected PrintStream log = System.out;
  
  public NoSQLJdbc(Connection conn)
  {
    this._conn = conn;
    if(useSequence(conn)) {
      if(debug) log.println(logprefix + "<init> dbms=" + _dbms + ",useSequence=true...");
    }
    else {
      if(debug) log.println(logprefix + "<init> dbms=" + _dbms + ",useSequence=false...");
    }
  }
  
  public NoSQLJdbc(Connection conn, boolean debug)
  {
    this._conn = conn;
    this.debug = debug;
    if(useSequence(conn)) {
      if(debug) log.println(logprefix + "<init> dbms=" + _dbms + ",useSequence=true...");
    }
    else {
      if(debug) log.println(logprefix + "<init> dbms=" + _dbms + ",useSequence=false...");
    }
  }
  
  public NoSQLJdbc(String dataSource)
  {
    this.dataSource = dataSource;
    if(useSequence()) {
      if(debug) log.println(logprefix + "<init> dbms=" + _dbms + ",useSequence=true...");
    }
    else {
      if(debug) log.println(logprefix + "<init> dbms=" + _dbms + ",useSequence=false...");
    }
  }
  
  public NoSQLJdbc(String dataSource, boolean debug)
  {
    this.dataSource = dataSource;
    this.debug = debug;
    if(useSequence()) {
      if(debug) log.println(logprefix + "<init> dbms=" + _dbms + ",useSequence=true...");
    }
    else {
      if(debug) log.println(logprefix + "<init> dbms=" + _dbms + ",useSequence=false...");
    }
  }

  @Override
  public void setDebug(boolean debug) {
    this.debug = debug;
  }

  @Override
  public boolean isDebug() {
    return this.debug;
  }
  
  @Override
  public void setLog(PrintStream log) {
    this.log = log != null ? log : System.out; 
  }

  @Override
  public Map<String, Object> load(Map<String, Object> mapOptions) throws Exception {
    if(debug) log.println(logprefix + "load(" + mapOptions + ")...");
    Map<String, Object> mapResult = new HashMap<String, Object>(2);
    Connection conn = null;
    try {
      conn = getConnection();
      
      DatabaseMetaData dbmd = conn.getMetaData();
      mapResult.put("name",    dbmd.getDatabaseProductName());
      mapResult.put("version", dbmd.getDatabaseProductVersion());
    }
    catch(Exception ex) {
      if(debug) log.println(logprefix + "load(" + mapOptions + "): " + ex);
      throw ex;
    }
    finally {
      closeConnection(conn);
    }
    if(debug) log.println(logprefix + "load(" + mapOptions + ") -> " + mapResult);
    return mapResult;
  }

  @Override
  public boolean save(Map<String, Object> mapOptions) throws Exception {
    if(debug) log.println(logprefix + "save(" + mapOptions + ")...");
    boolean result = false;
    if(debug) log.println(logprefix + "save(" + mapOptions + ") -> " + result);
    return result;
  }

  @Override
  public Map<String, Object> getInfo() throws Exception {
    if(debug) log.println(logprefix + "getInfo()...");
    Map<String, Object> mapResult = new HashMap<String, Object>(2);
    Connection conn = null;
    try {
      conn = getConnection();
      
      DatabaseMetaData dbmd = conn.getMetaData();
      mapResult.put("name",    dbmd.getDatabaseProductName());
      mapResult.put("version", dbmd.getDatabaseProductVersion());
    }
    catch(Exception ex) {
      if(debug) log.println(logprefix + "getInfo(): " + ex);
      throw ex;
    }
    finally {
      closeConnection(conn);
    }
    if(debug) log.println(logprefix + "getInfo() -> " + mapResult);
    return mapResult;
  }

  @Override
  public List<String> getCollections() throws Exception {
    if(debug) log.println(logprefix + "getCollections()...");
    List<String> listResult = new ArrayList<String>();
    
    String[] types = { "TABLE" };
    
    Connection conn = null;
    ResultSet rs = null;
    try {
      conn = getConnection();
      
      DatabaseMetaData dbmd = conn.getMetaData();
      
      rs = dbmd.getTables(null, null, null, types);
      while(rs.next()) {
        String tableName = rs.getString(3);
        if(tableName.indexOf('$') >= 0 || tableName.equals("PLAN_TABLE")) continue;
        if(_tablesPrefix != null && tableName.startsWith(_tablesPrefix)) {
          tableName = tableName.substring(_tablesPrefix.length());
        }
        else {
          listResult.add(tableName);
        }
      }
    }
    catch(Exception ex) {
      if(debug) log.println(logprefix + "getCollections(): " + ex);
      throw ex;
    }
    finally {
      if(rs != null) try { rs.close(); } catch(Exception ex) {}
      closeConnection(conn);
    }
    
    if(debug) log.println(logprefix + "getCollections() -> " + listResult);
    return listResult;
  }

  @Override
  public boolean drop(String collection) throws Exception {
    if(debug) log.println(logprefix + "drop(" + collection + ")...");
    boolean result = false;
    String table = _tablesPrefix != null ? _tablesPrefix + collection : collection;
    Connection conn = null;
    Statement stm = null;
    try {
      conn = getConnection();
      
      stm = conn.createStatement();
      stm.setQueryTimeout(_queryTimeout);
      
      result = stm.execute("DROP TABLE " + table.toUpperCase());
    }
    catch(Exception ex) {
      if(debug) log.println(logprefix + "drop(" + collection + "): " + ex);
      throw ex;
    }
    finally {
      if(stm != null) try { stm.close(); } catch(Exception ex) {}
      closeConnection(conn);
    }
    
    if(debug) log.println(logprefix + "drop(" + collection + ") -> " + result);
    return result;
  }

  @Override
  public String insert(String collection, Map<String, ?> mapData) throws Exception {
    if(debug) log.println(logprefix + "insert(" + collection + "," + mapData + ")...");
    String result = null;
    String table = _tablesPrefix != null ? _tablesPrefix + collection : collection;
    try {
      result = executeInsert(table, mapData);
    }
    catch(Exception ex) {
      if(debug) log.println(logprefix + "insert(" + collection + "," + mapData + "): " + ex);
      throw ex;
    }
    if(debug) log.println(logprefix + "insert(" + collection + "," + mapData + ") -> " + result);
    return result;
  }

  @Override
  public String insert(String collection, Map<String, ?> mapData, boolean refresh) throws Exception {
    if(debug) log.println(logprefix + "insert(" + collection + "," + mapData + "," + refresh + ")...");
    String result = null;
    String table = _tablesPrefix != null ? _tablesPrefix + collection : collection;
    try {
      result = executeInsert(table, mapData);
    }
    catch(Exception ex) {
      if(debug) log.println(logprefix + "insert(" + collection + "," + mapData + "," + refresh + "): " + ex);
      throw ex;
    }
    if(debug) log.println(logprefix + "insert(" + collection + "," + mapData + "," + refresh + ") -> " + result);
    return result;
  }

  @Override
  public int bulkIns(String collection, List<Map<String, ?>> listData) throws Exception {
    if(debug) {
      if(listData != null) {
        log.println(logprefix + "bulkIns(" + collection + ", " + listData.size() + " documents)...");
      }
      else {
        log.println(logprefix + "bulkIns(" + collection + ", null)...");
      }
    }
    if(listData == null || listData.size() == 0) {
      if(listData != null) {
        log.println(logprefix + "bulkIns(" + collection + ", " + listData.size() + " documents) -> 0");
      }
      else {
        log.println(logprefix + "bulkIns(" + collection + ", null) -> 0");
      }
      return 0;
    }
    int result = 0;
    String table = _tablesPrefix != null ? _tablesPrefix + collection : collection;
    try {
      for(int i = 0; i < listData.size(); i++) {
        executeInsert(table, listData.get(i));
        result++;
      }
    }
    catch(Exception ex) {
      log.println(logprefix + "bulkIns(" + collection + ", " + listData.size() + " documents): " + ex);
      throw ex;
    }
    
    log.println(logprefix + "bulkIns(" + collection + ", " + listData.size() + " documents) -> " + result);
    return result;
  }

  @Override
  public boolean replace(String collection, Map<String, ?> mapData, String id) throws Exception {
    if(debug) log.println(logprefix + "replace(" + collection + "," + mapData + ")...");
    boolean result = false;
    String table = _tablesPrefix != null ? _tablesPrefix + collection : collection;
    try {
      executeDelete(table, id);
      
      executeInsert(table, mapData);
    }
    catch(Exception ex) {
      if(debug) log.println(logprefix + "replace(" + collection + "," + mapData + "): " + ex);
      throw ex;
    }
    if(debug) log.println(logprefix + "replace(" + collection + "," + mapData + ") -> " + result);
    return result;
  }

  @Override
  public int update(String collection, Map<String, ?> mapData, String id) throws Exception {
    if(debug) log.println(logprefix + "update(" + collection + "," + mapData + "," + id + ")...");
    if(mapData == null || mapData.isEmpty()) {
      if(debug) log.println(logprefix + "update(" + collection + "," + mapData + "," + id + ") -> 0");
      return 0;
    }
    int result = 0;
    String table = _tablesPrefix != null ? _tablesPrefix + collection : collection;
    try {
      result = executeUpdate(table, mapData, id);
    }
    catch(Exception ex) {
      if(debug) log.println(logprefix + "update(" + collection + "," + mapData + "," + id + "): " + result);
      throw ex;
    }
    if(debug) log.println(logprefix + "update(" + collection + "," + mapData + "," + id + ") -> " + result);
    return result;
  }

  @Override
  public int update(String collection, Map<String, ?> mapData, Map<String, ?> mapFilter) throws Exception {
    if(debug) log.println(logprefix + "update(" + collection + "," + mapData + "," + mapFilter + ")...");
    if(mapData == null || mapData.isEmpty()) {
      if(debug) log.println(logprefix + "update(" + collection + "," + mapData + "," + mapFilter + ") -> 0");
      return 0;
    }
    int result = 0;
    String table = _tablesPrefix != null ? _tablesPrefix + collection : collection;
    try {
      result = executeUpdate(table, mapData, mapFilter);
    }
    catch(Exception ex) {
      if(debug) log.println(logprefix + "update(" + collection + "," + mapData + "," + mapFilter + "): " + ex);
      throw ex;
    }
    if(debug) log.println(logprefix + "update(" + collection + "," + mapData + "," + mapFilter + ") -> " + result);
    return result;
  }

  @Override
  public String upsert(String collection, Map<String, ?> mapData, Map<String, ?> mapFilter) throws Exception {
    if(debug) log.println(logprefix + "upsert(" + collection + "," + mapData + "," + mapFilter + ")...");
    String result = null;
    String table = _tablesPrefix != null ? _tablesPrefix + collection : collection;
    try {
      int upd = executeUpdate(table, mapData, mapFilter);
      if(upd > 0) {
        if(_idFieldName != null && _idFieldName.length() > 0) {
          result = WUtil.toString(mapFilter.get(_idFieldName), null);
        }
      }
      else {
        result = executeInsert(table, mapData);
      }
    }
    catch(Exception ex) {
      if(debug) log.println(logprefix + "upsert(" + collection + "," + mapData + "," + mapFilter + "): " + ex);
      throw ex;
    }
    if(debug) log.println(logprefix + "upsert(" + collection + "," + mapData + "," + mapFilter + ") -> " + result);
    return result;
  }

  @Override
  public int unset(String collection, String fields, String id) throws Exception {
    if(debug) log.println(logprefix + "unset(" + collection + "," + fields + "," + id + ")...");
    int result = 0;
    String table = _tablesPrefix != null ? _tablesPrefix + collection : collection;
    try {
      result = executeUnSet(table, fields, id);
    }
    catch(Exception ex) {
      if(debug) log.println(logprefix + "unset(" + collection + "," + fields + "," + id + "): " + ex);
      throw ex;
    }
    if(debug) log.println(logprefix + "unset(" + collection + "," + fields + "," + id + ") -> " + result);
    return result;
  }

  @Override
  public int inc(String collection, String id, String field, Number value) throws Exception {
    if(debug) log.println(logprefix + "inc(" + collection + "," + id + "," + field + "," + value + ")...");
    int result = 0;
    String table = _tablesPrefix != null ? _tablesPrefix + collection : collection;
    try {
      result = executeInc(table, id, field, value, null, null);
    }
    catch (Exception ex) {
      if(debug) log.println(logprefix + "inc(" + collection + "," + id + "," + field + "," + value + "): " + ex);
      throw ex;
    }
    if(debug) log.println(logprefix + "inc(" + collection + "," + id + "," + field + "," + value + ") -> " + result);
    return result;
  }

  @Override
  public int inc(String collection, String id, String field1, Number value1, String field2, Number value2) throws Exception {
    if(debug) log.println(logprefix + "inc(" + collection + "," + id + "," + field1 + "," + value1 + "," + field2 + "," + value2 + ")...");
    int result = 0;
    String table = _tablesPrefix != null ? _tablesPrefix + collection : collection;
    try {
      result = executeInc(table, id, field1, value1, field2, value2);
    }
    catch (Exception ex) {
      if(debug) log.println(logprefix + "inc(" + collection + "," + id + "," + field1 + "," + value1 + "," + field2 + "," + value2 + "): " + ex);
      throw ex;
    }
    if(debug) log.println(logprefix + "inc(" + collection + "," + id + "," + field1 + "," + value1 + "," + field2 + "," + value2 + ") -> " + result);
    return result;
  }

  @Override
  public int inc(String collection, Map<String, ?> mapFilter, String field, Number value) throws Exception {
    if(debug) log.println(logprefix + "inc(" + collection + "," + mapFilter + "," + field + "," + value + ")...");
    int result = 0;
    String table = _tablesPrefix != null ? _tablesPrefix + collection : collection;
    try {
      result = executeInc(table, mapFilter, field, value, null, null);
    }
    catch (Exception ex) {
      if(debug) log.println(logprefix + "inc(" + collection + "," + mapFilter + "," + field + "," + value + "): " + ex);
      throw ex;
    }
    if(debug) log.println(logprefix + "inc(" + collection + "," + mapFilter + "," + field + "," + value + ") -> " + result);
    return result;
  }

  @Override
  public int inc(String collection, Map<String, ?> mapFilter, String field1, Number value1, String field2, Number value2) throws Exception {
    if(debug) log.println(logprefix + "inc(" + collection + "," + mapFilter + "," + field1 + "," + value1 + "," + field2 + "," + value2 + ")...");
    int result = 0;
    String table = _tablesPrefix != null ? _tablesPrefix + collection : collection;
    try {
      result = executeInc(table, mapFilter, field1, value1, field2, value2);
    }
    catch (Exception ex) {
      if(debug) log.println(logprefix + "inc(" + collection + "," + mapFilter + "," + field1 + "," + value1 + "," + field2 + "," + value2 + "): " + ex);
      throw ex;
    }
    if(debug) log.println(logprefix + "inc(" + collection + "," + mapFilter + "," + field1 + "," + value1 + "," + field2 + "," + value2 + ") -> " + result);
    return result;
  }

  @Override
  public int delete(String collection, String id) throws Exception {
    if(debug) log.println(logprefix + "delete(" + collection + "," + id + ")...");
    if(collection == null || id == null) {
      if(debug) log.println(logprefix + "delete(" + collection + "," + id + ") -> -1");
      return -1;
    }
    int result = 0;
    String table = _tablesPrefix != null ? _tablesPrefix + collection : collection;
    try {
      result = executeDelete(table, id);
    }
    catch(Exception ex) {
      if(debug) log.println(logprefix + "delete(" + collection + "," + id + "): " + ex);
      throw ex;
    }
    if(debug) log.println(logprefix + "delete(" + collection + "," + id + ") -> " + result);
    return result;
  }

  @Override
  public int delete(String collection, Map<String, ?> mapFilter) throws Exception {
    if(debug) log.println(logprefix + "delete(" + collection + "," + mapFilter + ")...");
    if(collection == null || mapFilter == null) {
      if(debug) log.println(logprefix + "delete(" + collection + "," + mapFilter + ") -> -1");
      return -1;
    }
    int result = 0;
    String table = _tablesPrefix != null ? _tablesPrefix + collection : collection;
    try {
      result = executeDelete(table, mapFilter);
    }
    catch(Exception ex) {
      if(debug) log.println(logprefix + "delete(" + collection + "," + mapFilter + "): " + ex);
      throw ex;
    }
    if(debug) log.println(logprefix + "delete(" + collection + "," + mapFilter + ") -> " + result);
    return result;
  }

  @Override
  public List<Map<String, Object>> find(String collection, Map<String, ?> mapFilter, String fields) throws Exception {
    if(debug) log.println(logprefix + "find(" + collection + "," + mapFilter + ",\"" + fields + "\")...");
    List<Map<String, Object>> listResult = null;
    
    if(fields == null || fields.length() == 0) {
      fields = WUtil.toString(mapFilter.get(FILTER_FIELDS), null);
      if(fields == null || fields.length() == 0) {
        fields = "*";
      }
    }
    
    String table = _tablesPrefix != null ? _tablesPrefix + collection : collection;
    
    String sSQL = "SELECT " + fields.toUpperCase() + " FROM " + table.toUpperCase();
    String where = buildWhere(mapFilter);
    if(where != null && where.length() > 0) {
      sSQL += " WHERE " + where;
    }
    
    Connection conn = null;
    Statement stm = null;
    ResultSet rs = null;
    try {
      conn = getConnection();
      
      stm = conn.createStatement();
      stm.setQueryTimeout(_queryTimeout);
      
      if(debug) log.println(logprefix + "#  " + sSQL);
      rs = stm.executeQuery(sSQL);
      
      listResult = toList(rs, 0, 0);
    }
    catch(Exception ex) {
      if(debug) log.println(logprefix + "find(" + collection + "," + mapFilter + ",\"" + fields + "\"): " + ex);
      throw ex;
    }
    finally {
      if(rs  != null) try { rs.close();  } catch(Exception ex) {}
      if(stm != null) try { stm.close(); } catch(Exception ex) {}
      closeConnection(conn);
    }
    
    if(debug) log.println(logprefix + "find(" + collection + "," + mapFilter + ",\"" + fields + "\") -> " + listResult.size() + " documents");
    return listResult;
  }

  @Override
  public List<Map<String, Object>> find(String collection, Map<String, ?> mapFilter, String fields, String orderBy, int limit) throws Exception {
    if(debug) log.println(logprefix + "find(" + collection + "," + mapFilter + ",\"" + fields + "\",\"" + orderBy + "\"," + limit + ")...");
    List<Map<String, Object>> listResult = null;
    
    if(fields == null || fields.length() == 0) {
      fields = WUtil.toString(mapFilter.get(FILTER_FIELDS), null);
      if(fields == null || fields.length() == 0) {
        fields = "*";
      }
    }
    
    String table = _tablesPrefix != null ? _tablesPrefix + collection : collection;
    
    String sSQL = "SELECT " + fields.toUpperCase() + " FROM " + table.toUpperCase();
    String where = buildWhere(mapFilter);
    if(where != null && where.length() > 0) {
      sSQL += " WHERE " + where;
    }
    if(orderBy != null && orderBy.length() > 0) {
      sSQL += " ORDER BY " + orderBy;
    }
    
    Connection conn = null;
    Statement stm = null;
    ResultSet rs = null;
    try {
      conn = getConnection();
      
      stm = conn.createStatement();
      stm.setQueryTimeout(_queryTimeout);
      
      if(debug) log.println(logprefix + "#  " + sSQL);
      rs = stm.executeQuery(sSQL);
      
      listResult = toList(rs, limit, 0);
    }
    catch(Exception ex) {
      if(debug) log.println(logprefix + "find(" + collection + "," + mapFilter + ",\"" + fields + "\",\"" + orderBy + "\"," + limit + "): " + ex);
      throw ex;
    }
    finally {
      if(rs  != null) try { rs.close();  } catch(Exception ex) {}
      if(stm != null) try { stm.close(); } catch(Exception ex) {}
      closeConnection(conn);
    }
    
    if(debug) log.println(logprefix + "find(" + collection + "," + mapFilter + ",\"" + fields + "\",\"" + orderBy + "\"," + limit + ") -> " + listResult.size() + " documents");
    return listResult;
  }

  @Override
  public List<Map<String, Object>> find(String collection, Map<String, ?> mapFilter, String fields, String orderBy, int limit, int skip) throws Exception {
    if(debug) log.println(logprefix + "find(" + collection + "," + mapFilter + ",\"" + fields + "\",\"" + orderBy + "\"," + limit + "," + skip + ")...");
    List<Map<String, Object>> listResult = null;
    
    if(fields == null || fields.length() == 0) {
      fields = WUtil.toString(mapFilter.get(FILTER_FIELDS), null);
      if(fields == null || fields.length() == 0) {
        fields = "*";
      }
    }
    
    String table = _tablesPrefix != null ? _tablesPrefix + collection : collection;
    
    String sSQL = "SELECT " + fields.toUpperCase() + " FROM " + table.toUpperCase();
    String where = buildWhere(mapFilter);
    if(where != null && where.length() > 0) {
      sSQL += " WHERE " + where;
    }
    if(orderBy != null && orderBy.length() > 0) {
      sSQL += " ORDER BY " + orderBy;
    }
    
    Connection conn = null;
    Statement stm = null;
    ResultSet rs = null;
    try {
      conn = getConnection();
      
      stm = conn.createStatement();
      stm.setQueryTimeout(_queryTimeout);
      
      if(debug) log.println(logprefix + "#  " + sSQL);
      rs = stm.executeQuery(sSQL);
      
      listResult = toList(rs, limit, skip);
    }
    catch(Exception ex) {
      if(debug) log.println(logprefix + "find(" + collection + "," + mapFilter + ",\"" + fields + "\",\"" + orderBy + "\"," + limit + "," + skip + "): " + ex);
      throw ex;
    }
    finally {
      if(rs  != null) try { rs.close();  } catch(Exception ex) {}
      if(stm != null) try { stm.close(); } catch(Exception ex) {}
      closeConnection(conn);
    }
    
    if(debug) log.println(logprefix + "find(" + collection + "," + mapFilter + ",\"" + fields + "\",\"" + orderBy + "\"," + limit + "," + skip + ") -> " + listResult.size() + " documents");
    return listResult;
  }

  @Override
  public List<Map<String, Object>> search(String collection, String field, String text) throws Exception {
    if(debug) log.println(logprefix + "search(" + collection + "," + field + "," + text + ")...");
    List<Map<String, Object>> listResult = null;
    
    if(text == null) text = "";
    
    String table = _tablesPrefix != null ? _tablesPrefix + collection : collection;
    
    String sSQL = "SELECT * FROM " + table.toUpperCase();
    if(field != null && field.length() > 0) {
      sSQL += " WHERE " + field.toUpperCase() + " LIKE '%" + text.replace("'", "''").replace(" ", "%") + "%'";
    }
    
    Connection conn = null;
    Statement stm = null;
    ResultSet rs = null;
    try {
      conn = getConnection();
      
      stm = conn.createStatement();
      stm.setQueryTimeout(_queryTimeout);
      
      if(debug) log.println(logprefix + "#  " + sSQL);
      rs = stm.executeQuery(sSQL);
      
      listResult = toList(rs, 0, 0);
    }
    catch(Exception ex) {
      if(debug) log.println(logprefix + "search(" + collection + "," + field + "," + text + "): " + ex);
      throw ex;
    }
    finally {
      if(rs  != null) try { rs.close();  } catch(Exception ex) {}
      if(stm != null) try { stm.close(); } catch(Exception ex) {}
      closeConnection(conn);
    }
    
    if(debug) log.println(logprefix + "search(" + collection + "," + field + "," + text + ") -> " + listResult.size() + " documents");
    return listResult;
  }

  @Override
  public List<Map<String, Object>> group(String collection, Map<String, ?> mapFilter, String field, String groupFunction) throws Exception {
    if(debug) log.println(logprefix + "group(" + collection + "," + mapFilter + ",\"" + field + "\",\"" + groupFunction + "\")...");
    List<Map<String, Object>> listResult = null;
    
    if(field == null || field.length() == 0) {
      listResult = new ArrayList<Map<String, Object>>(0);
      if(debug) log.println(logprefix + "group(" + collection + "," + mapFilter + ",\"" + field + "\",\"" + groupFunction + "\") -> " + listResult.size() + " documents (field=" + field + ")");
      return listResult;
    }
    if(groupFunction == null || groupFunction.length() == 0) {
      groupFunction = "COUNT(*)";
    }
    int p = groupFunction.indexOf('(');
    if(p < 0) {
      char c0 = groupFunction.charAt(0);
      if(c0 == 'c' || c0 == 'C') {
        groupFunction += "(*)";
      }
    }
    
    String table = _tablesPrefix != null ? _tablesPrefix + collection : collection;
    
    String sSQL = "SELECT " + field.toUpperCase() + "," + groupFunction.toUpperCase() + " FROM " + table.toUpperCase();
    String where = buildWhere(mapFilter);
    if(where != null && where.length() > 0) {
      sSQL += " WHERE " + where;
    }
    sSQL += " GROUP BY " + field.toUpperCase();
    
    Connection conn = null;
    Statement stm = null;
    ResultSet rs = null;
    try {
      conn = getConnection();
      
      stm = conn.createStatement();
      stm.setQueryTimeout(_queryTimeout);
      
      if(debug) log.println(logprefix + "#  " + sSQL);
      rs = stm.executeQuery(sSQL);
      
      listResult = toList(rs, 0, 0);
    }
    catch(Exception ex) {
      if(debug) log.println(logprefix + "group(" + collection + "," + mapFilter + ",\"" + field + "\",\"" + groupFunction + "\"): " + ex);
      throw ex;
    }
    finally {
      if(rs  != null) try { rs.close();  } catch(Exception ex) {}
      if(stm != null) try { stm.close(); } catch(Exception ex) {}
      closeConnection(conn);
    }
    
    if(debug) log.println(logprefix + "group(" + collection + "," + mapFilter + ",\"" + field + "\",\"" + groupFunction + "\") -> " + listResult.size() + " documents");
    return listResult;
  }

  @Override
  public Map<String, Object> read(String collection, String id) throws Exception {
    if(debug) log.println(logprefix + "read(" + collection + "," + id + ")...");
    Map<String, Object> mapResult = null;
    if(_idFieldName == null || _idFieldName.length() == 0) {
      if(debug) log.println(logprefix + "read(" + collection + "," + id + ") -> " + mapResult + " (_idFieldName=" + _idFieldName + ")");
      return mapResult;
    }
    
    String table = _tablesPrefix != null ? _tablesPrefix + collection : collection;
    
    String sSQL = "SELECT * FROM " + table.toUpperCase();
    sSQL += " WHERE " + _idFieldName + "=";
    
    Object oId = getIdValue(id);
    
    Connection conn = null;
    PreparedStatement pstm = null;
    ResultSet rs = null;
    try {
      conn = getConnection();
      
      pstm = conn.prepareStatement(sSQL + "?");
      pstm.setQueryTimeout(_queryTimeout);
      if(oId instanceof Number) {
        pstm.setInt(1, ((Number) oId).intValue());
      }
      else {
        pstm.setString(1, id);
      }
      if(debug) log.println(logprefix + "#  " + sSQL + toSQL(oId));
      rs = pstm.executeQuery();
      
      if(rs.next()) {
        mapResult = toMap(rs);
      }
    }
    catch(Exception ex) {
      if(debug) log.println(logprefix + "read(" + collection + "," + id + "): " + ex);
      throw ex;
    }
    finally {
      if(rs   != null) try { rs.close();   } catch(Exception ex) {}
      if(pstm != null) try { pstm.close(); } catch(Exception ex) {}
      closeConnection(conn);
    }
    
    if(debug) log.println(logprefix + "read(" + collection + "," + id + ") -> " + mapResult);
    return mapResult;
  }

  @Override
  public int count(String collection, Map<String, ?> mapFilter) throws Exception {
    if(debug) log.println(logprefix + "count(" + collection + "," + mapFilter + ")...");
    int result = 0;
    
    String table = _tablesPrefix != null ? _tablesPrefix + collection : collection;
    
    String sSQL = "SELECT COUNT(*) FROM " + table.toUpperCase();
    String where = buildWhere(mapFilter);
    if(where != null && where.length() > 0) {
      sSQL += " WHERE " + where;
    }
    
    Connection conn = null;
    Statement stm = null;
    ResultSet rs = null;
    try {
      conn = getConnection();
      
      stm = conn.createStatement();
      stm.setQueryTimeout(_queryTimeout);
      
      if(debug) log.println(logprefix + "#  " + sSQL);
      rs = stm.executeQuery(sSQL);
      if(rs.next()) {
        result = rs.getInt(1);
      }
    }
    catch(Exception ex) {
      if(debug) log.println(logprefix + "count(" + collection + "," + mapFilter + "): " + ex);
      throw ex;
    }
    finally {
      if(rs  != null) try { rs.close();  } catch(Exception ex) {}
      if(stm != null) try { stm.close(); } catch(Exception ex) {}
      closeConnection(conn);
    }
    
    if(debug) log.println(logprefix + "count(" + collection + "," + mapFilter + ") -> " + result);
    return result;
  }

  @Override
  public
  boolean createIndex(String collection, String field, int type) throws Exception {
    if(debug) log.println(logprefix + "createIndex(" + collection + "," + field + "," + type + ")...");
    
    String table = _tablesPrefix != null ? _tablesPrefix + collection : collection;
    
    String sSQL = "CREATE INDEX IDX" + System.currentTimeMillis() + " ON " + table.toUpperCase() + "(" + field.toUpperCase() + ")";
    
    boolean result = true;
    Connection conn = null;
    Statement stm = null;
    try {
      conn = getConnection();
      
      stm = conn.createStatement();
      stm.setQueryTimeout(_queryTimeout);
      
      if(debug) log.println(logprefix + "#  " + sSQL);
      stm.execute(sSQL);
    }
    catch(Exception ex) {
      if(debug) log.println(logprefix + "createIndex(" + collection + "," + field + "," + type + "): " + ex);
      throw ex;
    }
    finally {
      if(stm != null) try { stm.close(); } catch(Exception ex) {}
      closeConnection(conn);
    }
    
    if(debug) log.println(logprefix + "createIndex(" + collection + "," + field + "," + type + ") -> " + result);
    return result;
  }
  
  @Override
  public List<Map<String, Object>> listIndexes(String collection) throws Exception {
    if(debug) log.println(logprefix + "listIndexes(" + collection + ")...");
    List<Map<String, Object>> listResult = null;
    
    String table = _tablesPrefix != null ? _tablesPrefix + collection : collection;
    
    Connection conn = null;
    ResultSet rs = null;
    try {
      conn = getConnection();
      
      DatabaseMetaData dbmd = conn.getMetaData();
      rs = dbmd.getIndexInfo(null, null, table, true, true);
      
      listResult = toList(rs, 0, 0);
    }
    catch(Exception ex) {
      if(debug) log.println(logprefix + "listIndexes(" + collection + "): " + ex);
      throw ex;
    }
    finally {
      if(rs != null) try { rs.close(); } catch(Exception ex) {}
      closeConnection(conn);
    }
    
    if(debug) log.println(logprefix + "listIndexes(" + collection + ") -> " + listResult);
    return listResult;
  }

  @Override
  public String writeFile(String filename, byte[] content, Map<String, ?> mapMetadata) throws Exception {
    return writeFile(filename, content, mapMetadata, null);
  }
  
  @Override
  public String writeFile(String filename, byte[] content, Map<String, ?> mapMetadata, Map<String, ?> mapAttributes) throws Exception {
    if(debug) {
      if(content == null) {
        log.println(logprefix + "writeFile(" + filename + ",null," + mapMetadata + "," + mapAttributes + ")...");
      }
      else {
        log.println(logprefix + "writeFile(" + filename + ",byte[" + content.length + "]," + mapMetadata + "," + mapAttributes + ")...");
      }
    }
    String result = null;
    if(filename == null || filename.length() == 0) {
      if(content == null) {
        log.println(logprefix + "writeFile(" + filename + ",null," + mapMetadata + "," + mapAttributes + ") -> " + result + " (filename=" + filename + ")");
      }
      else {
        log.println(logprefix + "writeFile(" + filename + ",byte[" + content.length + "]," + mapMetadata + "," + mapAttributes + ") -> " + result + " (filename=" + filename + ")");
      }
      return result;
    }
    
    String sSQL = "INSERT INTO " + _filesTable + "(" + _filesNameField + ") VALUES";
    Connection conn = null;
    PreparedStatement pstm = null;
    try {
      conn = getConnection();
      
      pstm = conn.prepareStatement(sSQL + "(?)");
      pstm.setQueryTimeout(_queryTimeout);
      
      pstm.setString(1, filename);
      if(debug) log.println(logprefix + "#  " + sSQL + "(" + toSQL(filename) + ")");
      pstm.executeUpdate();
      
      boolean res = setBLOBContent(conn, filename, content);
      
      result = res ? filename : null;
    }
    catch(Exception ex) {
      if(debug) log.println(logprefix + "writeFile(" + filename + ",null," + mapMetadata + "," + mapAttributes + "): " + ex);
      throw ex;
    }
    finally {
      if(pstm != null) try { pstm.close(); } catch(Exception ex) {}
      closeConnection(conn);
    }
    
    if(debug) {
      if(content == null) {
        log.println(logprefix + "writeFile(" + filename + ",null," + mapMetadata + "," + mapAttributes + ") -> " + result);
      }
      else {
        log.println(logprefix + "writeFile(" + filename + ",byte[" + content.length + "]," + mapMetadata + "," + mapAttributes + ") -> " + result);
      }
    }
    return result;
  }

  @Override
  public List<Map<String, Object>> findFiles(String filename, Map<String, ?> mapFilter) throws Exception {
    if(debug) log.println(logprefix + "findFiles(" + filename + "," + mapFilter + ")...");
    List<Map<String, Object>> listResult = new ArrayList<Map<String, Object>>();
    
    String sSQL = "SELECT * FROM " + _filesTable;
    
    String where = buildWhere(mapFilter);
    boolean whereIsNotBlank = where != null && where.length() > 0;
    if(whereIsNotBlank) {
      sSQL += " WHERE " + where;
    }
    if(filename != null && filename.length() > 0) {
      filename = filename.replace("*", "%");
      if(whereIsNotBlank) {
        sSQL += " AND ";
      }
      else {
        sSQL += " WHERE ";
      }
      if(filename.indexOf('%') >= 0) {
        sSQL +=  _filesNameField + " LIKE '" + filename.replace("'", "''") + "'";
      }
      else {
        sSQL += _filesNameField + "='" + filename.replace("'", "''") + "'";
      }
    }
    
    Connection conn = null;
    Statement stm = null;
    ResultSet rs = null;
    try {
      conn = getConnection();
      
      stm = conn.createStatement();
      stm.setQueryTimeout(_queryTimeout);
      
      if(debug) log.println(logprefix + "#  " + sSQL);
      rs = stm.executeQuery(sSQL);
      while(rs.next()) {
        Map<String, Object> mapRecord = toMap(rs, true);
        mapRecord.put(FILE_CONTENT, mapRecord.remove(_filesContField));
        mapRecord.put(FILE_NAME,    mapRecord.remove(_filesNameField));
        listResult.add(mapRecord);
      }
    }
    catch(Exception ex) {
      if(debug) log.println(logprefix + "findFiles(" + filename + "," + mapFilter + "): " + ex);
      throw ex;
    }
    finally {
      if(rs  != null) try { rs.close();  } catch(Exception ex) {}
      if(stm != null) try { stm.close(); } catch(Exception ex) {}
      closeConnection(conn);
    }
    
    if(debug) log.println(logprefix + "findFiles(" + filename + "," + mapFilter + ") -> " + listResult.size() + " files");
    return listResult;
  }

  @Override
  public Map<String, Object> readFile(String filename) throws Exception {
    if(debug) log.println(logprefix + "readFile(" + filename + ")...");
    Map<String, Object> mapResult = null;
    
    String sSQL = "SELECT * FROM " + _filesTable + " WHERE " + _filesNameField + "=";
    
    Connection conn = null;
    PreparedStatement pstm = null;
    ResultSet rs = null;
    try {
      conn = getConnection();
      
      pstm = conn.prepareStatement(sSQL + "?");
      pstm.setQueryTimeout(_queryTimeout);
      pstm.setString(1, filename);
      
      if(debug) log.println(logprefix + "#  " + sSQL + toSQL(filename));
      rs = pstm.executeQuery();
      if(rs.next()) {
        mapResult = toMap(rs, true);
      }
      
      if(mapResult != null) {
        mapResult.put(FILE_CONTENT, mapResult.remove(_filesContField));
        mapResult.put(FILE_NAME,    mapResult.remove(_filesNameField));
      }
    }
    catch(Exception ex) {
      if(debug) log.println(logprefix + "readFile(" + filename + "): " + ex);
      throw ex;
    }
    finally {
      if(rs   != null) try { rs.close();   } catch(Exception ex) {}
      if(pstm != null) try { pstm.close(); } catch(Exception ex) {}
      closeConnection(conn);
    }
    
    if(debug) log.println(logprefix + "readFile(" + filename + ") -> {" + mapResult.size() + "}");
    return mapResult;
  }

  @Override
  public boolean removeFile(String filename) throws Exception {
    if(debug) log.println(logprefix + "removeFile(" + filename + ")...");
    boolean result = false;
    if(filename == null || filename.length() == 0) {
      if(debug) log.println(logprefix + "removeFile(" + filename + ") -> " + result + " (filename=" + filename + ")");
      return result;
    }
    String sSQL = "DELETE FROM " + _filesTable + " WHERE " + _filesNameField + "=";
    Connection conn = null;
    PreparedStatement pstm = null;
    try {
      conn = getConnection();
      
      pstm = conn.prepareStatement(sSQL + "?");
      pstm.setQueryTimeout(_queryTimeout);
      pstm.setString(1, filename);
      
      if(debug) log.println(logprefix + "#  " + sSQL + toSQL(filename));
      int upd = pstm.executeUpdate();
      result = upd > 0;
    }
    catch(Exception ex) {
      if(debug) log.println(logprefix + "removeFile(" + filename + "): " + ex);
      throw ex;
    }
    finally {
      if(pstm != null) try { pstm.close(); } catch(Exception ex) {}
      closeConnection(conn);
    }
    if(debug) log.println(logprefix + "removeFile(" + filename + ") -> " + result);
    return result;
  }

  @Override
  public boolean renameFile(String filename, String newFilename) throws Exception {
    if(debug) log.println(logprefix + "renameFile(" + filename + "," + newFilename + ")...");
    boolean result = false;
    if(filename == null || filename.length() == 0) {
      if(debug) log.println(logprefix + "renameFile(" + filename + "," + newFilename + ") -> " + result + " (filename=" + filename + ")");
      return result;
    }
    if(newFilename == null || newFilename.length() == 0) {
      if(debug) log.println(logprefix + "renameFile(" + filename + "," + newFilename + ") -> " + result + " (newFilename=" + newFilename + ")");
      return result;
    }
    String sSQL_S = "UPDATE " + _filesTable + " SET " + _filesNameField + "=";
    String sSQL_W = " WHERE " + _filesNameField + "=";
    Connection conn = null;
    PreparedStatement pstm = null;
    try {
      conn = getConnection();
      
      pstm = conn.prepareStatement(sSQL_S + "?" + sSQL_W + "?");
      pstm.setQueryTimeout(_queryTimeout);
      pstm.setString(1, newFilename);
      pstm.setString(2, filename);
      
      if(debug) log.println(logprefix + "#  " + sSQL_S + toSQL(newFilename) + sSQL_W + toSQL(filename));
      int upd = pstm.executeUpdate();
      result = upd > 0;
    }
    catch(Exception ex) {
      if(debug) log.println(logprefix + "renameFile(" + filename + "," + newFilename + "): " + ex);
      throw ex;
    }
    finally {
      if(pstm != null) try { pstm.close(); } catch(Exception ex) {}
      closeConnection(conn);
    }
    if(debug) log.println(logprefix + "renameFile(" + filename + "," + newFilename + ") -> " + result);
    return result;
  }
  
  protected
  String executeInsert(String table, Map<String, ?> mapValues)
    throws Exception
  {
    if(mapValues == null || mapValues.isEmpty()) {
      return null;
    }
    
    List<String> listFields = getFields(mapValues);
    if(listFields == null || listFields.size() == 0) {
      return null;
    }
    
    Object id = null;
    int result = 0;
    Connection conn = null;
    PreparedStatement pstm = null;
    try {
      conn = getConnection();
      
      Object nextValResult = null;
      if(_idFieldName != null && _idFieldName.length() > 0) {
        if(listFields.contains(_idFieldName)) {
          id = mapValues.get(_idFieldName);
        }
        else {
          listFields.add(0, _idFieldName);
          if(_numSequence != null && _numSequence.booleanValue()) {
            nextValResult = nextVal(conn, _sequencePrefix + table.toUpperCase() + _sequenceSuffix);
          }
          else {
            nextValResult = nextValString();
          }
          id = nextValResult;
        }
      }
      
      String sSQL_I = "INSERT INTO " + table.toUpperCase();
      String sFields = "";
      for(int i = 0; i < listFields.size(); i++) {
        sFields += "," + listFields.get(i).toUpperCase();
      }
      sSQL_I += "(" + sFields.substring(1) + ")";
      
      String sParams = "";
      String sValues = "";
      for(int i = 0; i < listFields.size(); i++) sParams += ",?";
      
      pstm = conn.prepareStatement(sSQL_I + " VALUES (" + sParams.substring(1) + ")");
      pstm.setQueryTimeout(_queryTimeout);
      for(int i = 0; i < listFields.size(); i++) {
        String field = listFields.get(i);
        
        Object parameter = null;
        if(i == 0 && nextValResult != null) {
          parameter = nextValResult;
        }
        else {
          parameter = mapValues.get(field);
        }
        
        if(debug) sValues += "," + toSQL(parameter);
        
        if(parameter instanceof Integer) {
          pstm.setInt(i + 1, ((Integer) parameter).intValue());
        }
        else if(parameter instanceof Long) {
          pstm.setLong(i + 1, ((Long) parameter).longValue());
        }
        else if(parameter instanceof Double) {
          pstm.setDouble(i + 1, ((Double) parameter).doubleValue());
        }
        else if(parameter instanceof String) {
          pstm.setString(i + 1, (String) parameter);
        }
        else if(parameter instanceof Boolean) {
          Object booleanValue = getBooleanValue(parameter);
          if(booleanValue instanceof Number) {
            pstm.setInt(i + 1, ((Number) booleanValue).intValue());
          }
          else if(booleanValue instanceof String) {
            pstm.setString(i + 1, (String) booleanValue);
          }
          else {
            pstm.setInt(i + 1, ((Boolean) parameter).booleanValue() ? 1 : 0);
          }
        }
        else if(parameter instanceof java.util.Calendar) {
          pstm.setDate(i + 1, new java.sql.Date(((java.util.Calendar) parameter).getTimeInMillis()));
        }
        else if(parameter instanceof java.sql.Date) {
          pstm.setDate(i + 1, (java.sql.Date) parameter);
        }
        else if(parameter instanceof java.sql.Timestamp) {
          pstm.setTimestamp(i + 1, (java.sql.Timestamp) parameter);
        }
        else if(parameter instanceof java.sql.Time) {
          pstm.setTime(i + 1, (java.sql.Time) parameter);
        }
        else if(parameter instanceof java.util.Date) {
          pstm.setDate(i + 1, new java.sql.Date(((java.util.Date) parameter).getTime()));
        }
        else {
          pstm.setString(i + 1, JSON.stringify(parameter));
        }
      }
      
      if(debug) log.println(logprefix + "#  " + sSQL_I + " VALUES (" + sValues.substring(1) + ")");
      result = pstm.executeUpdate();
    }
    finally {
      if(pstm != null) try { pstm.close(); } catch(Exception ex) {}
      closeConnection(conn);
    }
    return result > 0 ? String.valueOf(id) : null;
  }
  
  protected
  int executeUpdate(String table, Map<String, ?> mapValues, String id)
    throws Exception
  {
    if(mapValues == null || mapValues.isEmpty()) {
      return 0;
    }
    if(_idFieldName == null || _idFieldName.length() == 0) {
      if(debug) log.println(logprefix + "executeUpdate(" + table + "," + mapValues + "," + id + ") -> 0 (_idFieldName=" + _idFieldName + ")");
      return 0;
    }
    
    List<String> listFields = getFields(mapValues);
    if(listFields == null || listFields.size() == 0) {
      return 0;
    }
    
    Object oId = null;
    if(_idFieldName != null && _idFieldName.length() > 0) {
      if(!listFields.contains(_idFieldName)) {
        listFields.add(_idFieldName);
        int iId = WUtil.toInt(id, 0);
        if(iId != 0) {
          oId = iId;
        }
        else {
          oId = id;
        }
      }
    }
    
    int size = listFields.size();
    
    String sSQL_U = "UPDATE " + table.toUpperCase() + " SET ";
    
    String sParams = "";
    String sValues = "";
    for(int i = 0; i < size - 1; i++) {
      sParams += "," + listFields.get(i).toUpperCase() + "=?";
    }
    
    String sSQL_W = " WHERE " + _idFieldName + "=";
    
    int result = 0;
    Connection conn = null;
    PreparedStatement pstm = null;
    try {
      conn = getConnection();
      
      pstm = conn.prepareStatement(sSQL_U + sParams.substring(1) + sSQL_W + "?");
      pstm.setQueryTimeout(_queryTimeout);
      for(int i = 0; i < size; i++) {
        String field = listFields.get(i);
        
        Object parameter = null;
        if(i == size - 1 && oId != null) {
          parameter = oId;
        }
        else {
          parameter = mapValues.get(field);
        }
        
        if(debug) sValues += "," + field.toUpperCase() + "=" + toSQL(parameter);
        
        if(parameter instanceof Integer) {
          pstm.setInt(i + 1, ((Integer) parameter).intValue());
        }
        else if(parameter instanceof Long) {
          pstm.setLong(i + 1, ((Long) parameter).longValue());
        }
        else if(parameter instanceof Double) {
          pstm.setDouble(i + 1, ((Double) parameter).doubleValue());
        }
        else if(parameter instanceof String) {
          pstm.setString(i + 1, (String) parameter);
        }
        else if(parameter instanceof Boolean) {
          Object booleanValue = getBooleanValue(parameter);
          if(booleanValue instanceof Number) {
            pstm.setInt(i + 1, ((Number) booleanValue).intValue());
          }
          else if(booleanValue instanceof String) {
            pstm.setString(i + 1, (String) booleanValue);
          }
          else {
            pstm.setInt(i + 1, ((Boolean) parameter).booleanValue() ? 1 : 0);
          }
        }
        else if(parameter instanceof java.util.Calendar) {
          pstm.setDate(i + 1, new java.sql.Date(((java.util.Calendar) parameter).getTimeInMillis()));
        }
        else if(parameter instanceof java.sql.Date) {
          pstm.setDate(i + 1, (java.sql.Date) parameter);
        }
        else if(parameter instanceof java.sql.Timestamp) {
          pstm.setTimestamp(i + 1, (java.sql.Timestamp) parameter);
        }
        else if(parameter instanceof java.sql.Time) {
          pstm.setTime(i + 1, (java.sql.Time) parameter);
        }
        else if(parameter instanceof java.util.Date) {
          pstm.setDate(i + 1, new java.sql.Date(((java.util.Date) parameter).getTime()));
        }
        else {
          pstm.setString(i + 1, JSON.stringify(parameter));
        }
      }
      
      if(debug) log.println(logprefix + "#  " + sSQL_U + sValues.substring(1) + sSQL_W + toSQL(oId));
      result = pstm.executeUpdate();
    }
    finally {
      if(pstm != null) try { pstm.close(); } catch(Exception ex) {}
      closeConnection(conn);
    }
    return result;
  }
  
  protected
  int executeUpdate(String table, Map<String, ?> mapValues, Map<String, ?> mapFilter)
    throws Exception
  {
    if(mapValues == null || mapValues.isEmpty()) {
      return 0;
    }
    
    List<String> listFields = getFields(mapValues);
    if(listFields == null || listFields.size() == 0) {
      return 0;
    }
    
    String where = buildWhere(mapFilter);
    
    int size = listFields.size();
    
    String sSQL = "UPDATE " + table.toUpperCase() + " SET ";
    String sFields = "";
    for(int i = 0; i < size - 1; i++) {
      sFields += "," + listFields.get(i).toUpperCase() + "=?";
    }
    sSQL += sFields.substring(1);
    if(where != null && where.length() > 0) {
      sSQL += " WHERE " + where;
    }
    
    int result = 0;
    Connection conn = null;
    PreparedStatement pstm = null;
    try {
      conn = getConnection();
      
      pstm = conn.prepareStatement(sSQL);
      pstm.setQueryTimeout(_queryTimeout);
      for(int i = 0; i < size; i++) {
        String field = listFields.get(i);
        
        Object parameter = mapValues.get(field);
        
        if(parameter instanceof Integer) {
          pstm.setInt(i + 1, ((Integer) parameter).intValue());
        }
        else if(parameter instanceof Long) {
          pstm.setLong(i + 1, ((Long) parameter).longValue());
        }
        else if(parameter instanceof Double) {
          pstm.setDouble(i + 1, ((Double) parameter).doubleValue());
        }
        else if(parameter instanceof String) {
          pstm.setString(i + 1, (String) parameter);
        }
        else if(parameter instanceof Boolean) {
          Object booleanValue = getBooleanValue(parameter);
          if(booleanValue instanceof Number) {
            pstm.setInt(i + 1, ((Number) booleanValue).intValue());
          }
          else if(booleanValue instanceof String) {
            pstm.setString(i + 1, (String) booleanValue);
          }
          else {
            pstm.setInt(i + 1, ((Boolean) parameter).booleanValue() ? 1 : 0);
          }
        }
        else if(parameter instanceof java.util.Calendar) {
          pstm.setDate(i + 1, new java.sql.Date(((java.util.Calendar) parameter).getTimeInMillis()));
        }
        else if(parameter instanceof java.sql.Date) {
          pstm.setDate(i + 1, (java.sql.Date) parameter);
        }
        else if(parameter instanceof java.sql.Timestamp) {
          pstm.setTimestamp(i + 1, (java.sql.Timestamp) parameter);
        }
        else if(parameter instanceof java.sql.Time) {
          pstm.setTime(i + 1, (java.sql.Time) parameter);
        }
        else if(parameter instanceof java.util.Date) {
          pstm.setDate(i + 1, new java.sql.Date(((java.util.Date) parameter).getTime()));
        }
        else {
          pstm.setString(i + 1, JSON.stringify(parameter));
        }
      }
      
      if(debug) log.println(logprefix + "#  " + sSQL);
      result = pstm.executeUpdate();
    }
    finally {
      if(pstm != null) try { pstm.close(); } catch(Exception ex) {}
      closeConnection(conn);
    }
    return result;
  }
  
  protected
  int executeDelete(String table, String id)
    throws Exception
  {
    if(id == null || id.length() == 0) {
      return 0;
    }
    if(_idFieldName == null || _idFieldName.length() == 0) {
      if(debug) log.println(logprefix + "executeDelete(" + table + "," + id + ") -> 0 (_idFieldName=" + _idFieldName + ")");
      return 0;
    }
    
    Object oId = getIdValue(id);
    
    String sSQL = "DELETE FROM " + table.toUpperCase() + " WHERE " + _idFieldName + "=";
    
    int result = 0;
    Connection conn = null;
    PreparedStatement pstm = null;
    try {
      conn = getConnection();
      
      pstm = conn.prepareStatement(sSQL + "?");
      pstm.setQueryTimeout(_queryTimeout);
      if(oId instanceof Number) {
        pstm.setInt(1, ((Number) oId).intValue());
      }
      else {
        pstm.setString(1, id.toString());
      }
      
      if(debug) log.println(logprefix + "#  " + sSQL + toSQL(oId));
      result = pstm.executeUpdate();
    }
    finally {
      if(pstm != null) try { pstm.close(); } catch(Exception ex) {}
      closeConnection(conn);
    }
    return result;
  }
  
  protected
  int executeDelete(String table, Map<String, ?> mapFilter)
    throws Exception
  {
    String where = buildWhere(mapFilter);
    
    String sSQL = "DELETE FROM " + table.toUpperCase();
    if(where != null && where.length() > 0) {
      sSQL += " WHERE " + where;
    }
    int result = 0;
    Connection conn = null;
    PreparedStatement pstm = null;
    try {
      conn = getConnection();
      
      pstm = conn.prepareStatement(sSQL);
      pstm.setQueryTimeout(_queryTimeout);
      
      if(debug) log.println(logprefix + "#  " + sSQL);
      result = pstm.executeUpdate();
    }
    finally {
      if(pstm != null) try { pstm.close(); } catch(Exception ex) {}
      closeConnection(conn);
    }
    return result;
  }
  
  protected
  int executeUnSet(String table, String fields, String id)
    throws Exception
  {
    if(fields == null || fields.length() == 0) {
      if(debug) log.println(logprefix + "executeUnSet(" + table + "," + fields + "," + id + ") -> 0 (fields=" + fields + ")");
      return 0;
    }
    
    if(_idFieldName == null && _idFieldName.length() == 0) {
      if(debug) log.println(logprefix + "executeUnSet(" + table + "," + fields + "," + id + ") -> 0 (_idFieldName=" + _idFieldName + ")");
      return 0;
    }
    
    String[] asFields = WUtil.toArrayOfString(fields, false);
    
    String sSQL = "UPDATE " + table.toUpperCase() + " SET ";
    String sFields = "";
    for(int i = 0; i < asFields.length; i++) {
      sFields += "," + asFields[i].toUpperCase() + "=NULL";
    }
    sSQL += sFields.substring(1);
    sSQL += " WHERE " + _idFieldName + "=";
    
    Object oId = getIdValue(id);
    
    int result = 0;
    Connection conn = null;
    PreparedStatement pstm = null;
    try {
      conn = getConnection();
      
      pstm = conn.prepareStatement(sSQL + "?");
      pstm.setQueryTimeout(_queryTimeout);
      
      if(oId instanceof Number) {
        pstm.setInt(1, ((Number) oId).intValue());
      }
      else {
        pstm.setString(1, id);
      }
      
      if(debug) log.println(logprefix + "#  " + sSQL + toSQL(oId));
      result = pstm.executeUpdate();
    }
    finally {
      if(pstm != null) try { pstm.close(); } catch(Exception ex) {}
      closeConnection(conn);
    }
    return result;
  }
  
  protected
  int executeInc(String table, String id, String field1, Number value1, String field2, Number value2)
    throws Exception
  {
    if(field1 == null || field1.length() == 0) {
      if(debug) log.println(logprefix + "executeInc(" + table + "," + id + "," + field1 + "," + value1 + "," + field2 + "," + value2 + ") -> 0 (field1=" + field1 + ")");
      return 0;
    }
    
    if(_idFieldName == null && _idFieldName.length() == 0) {
      if(debug) log.println(logprefix + "executeInc(" + table + "," + id + "," + field1 + "," + value1 + "," + field2 + "," + value2 + ") -> 0 (_idFieldName=" + _idFieldName + ")");
      return 0;
    }
    
    String sSQL = "UPDATE " + table.toUpperCase() + " SET ";
    sSQL += field1.toUpperCase() + "=" + field1.toUpperCase() + "+" + value1;
    if(field2 != null && field2.length() > 0) {
      sSQL += "," + field2.toUpperCase() + "=" + field2.toUpperCase() + "+" + value2;
    }
    if(_idFieldName != null && _idFieldName.length() > 0) {
      sSQL += " WHERE " + _idFieldName + "=";
    }
    
    Object oId = getIdValue(id);
    
    int result = 0;
    Connection conn = null;
    PreparedStatement pstm = null;
    try {
      conn = getConnection();
      
      pstm = conn.prepareStatement(sSQL + "?");
      pstm.setQueryTimeout(_queryTimeout);
      
      if(oId instanceof Number) {
        pstm.setInt(1, ((Number) oId).intValue());
      }
      else {
        pstm.setString(1, id);
      }
      
      if(debug) log.println(logprefix + "#  " + sSQL + toSQL(oId));
      result = pstm.executeUpdate();
    }
    finally {
      if(pstm != null) try { pstm.close(); } catch(Exception ex) {}
      closeConnection(conn);
    }
    return result;
  }
  
  protected
  int executeInc(String table, Map<String, ?> mapFilter, String field1, Number value1, String field2, Number value2)
    throws Exception
  {
    if(field1 == null || field1.length() == 0) {
      if(debug) log.println(logprefix + "executeInc(" + table + "," + mapFilter + "," + field1 + "," + value1 + "," + field2 + "," + value2 + ") -> 0 (field1=" + field1 + ")");
      return 0;
    }
    
    String where = buildWhere(mapFilter);
    
    String sSQL = "UPDATE " + table.toUpperCase() + " SET ";
    sSQL += field1.toUpperCase() + "=" + field1.toUpperCase() + "+" + value1;
    if(field2 != null && field2.length() > 0) {
      sSQL += "," + field2.toUpperCase() + "=" + field2.toUpperCase() + "+" + value2;
    }
    if(where != null && where.length() > 0) {
      sSQL += " WHERE " + where;
    }
    
    int result = 0;
    Connection conn = null;
    PreparedStatement pstm = null;
    try {
      conn = getConnection();
      
      pstm = conn.prepareStatement(sSQL);
      pstm.setQueryTimeout(_queryTimeout);
      
      if(debug) log.println(logprefix + "#  " + sSQL);
      result = pstm.executeUpdate();
    }
    finally {
      if(pstm != null) try { pstm.close(); } catch(Exception ex) {}
      closeConnection(conn);
    }
    return result;
  }
  
  protected
  String buildWhere(Map<String, ?> mapFilter)
  {
    if(mapFilter == null || mapFilter.isEmpty()) return null;
    
    StringBuilder sbResult = new StringBuilder();
    Iterator<String> iterator = mapFilter.keySet().iterator();
    while(iterator.hasNext()) {
      String key = iterator.next();
      Object valueTmp = mapFilter.get(key);
      if(valueTmp == null) continue;
      
      boolean boStartsWithPerc = false;
      boolean boEndsWithPerc   = false;
      boStartsWithPerc = key.startsWith("%");
      if(boStartsWithPerc) key = key.substring(3);
      boEndsWithPerc = key.endsWith("%");
      if(boEndsWithPerc) key = key.substring(0, key.length() - 1);
      
      boolean boGTE  = key.endsWith(">=");
      boolean boLTE  = key.endsWith("<=");
      boolean boNE   = key.endsWith("!=");
      if(!boNE) boNE = key.endsWith("<>");
      if(boGTE || boLTE || boNE) key = key.substring(0, key.length() - 2);
      
      boolean boGT  = key.endsWith(">");
      boolean boLT  = key.endsWith("<");
      if(boGT || boLT) key = key.substring(0, key.length() - 1);
      
      boolean boLike = false;
      String value   = null;
      if(valueTmp instanceof String) {
        String s = ((String) valueTmp).trim();
        
        if(s.length() == 0) continue;
        if(s.equalsIgnoreCase("null")) {
          value = "NULL";
        }
        else {
          value = "'";
          if(boStartsWithPerc) value += "%";
          value += s.replace("'", "''");
          if(boEndsWithPerc) value += "%";
          value += "'";
        }
        boLike = value.indexOf('%') >= 0 || value.indexOf('*') >= 0;
        
        // Is a date?
        char c0 = s.charAt(0);
        char cL = s.charAt(s.length()-1);
        if(!boLike && Character.isDigit(c0) && Character.isDigit(cL) && s.length() > 7 && s.length() < 11) {
          int iSep1 = s.indexOf('/');
          if(iSep1 < 0) {
            iSep1 = s.indexOf('-');
            // YYYY-MM-DD
            if(iSep1 != 4) iSep1 = -1;
          }
          if(iSep1 > 0) {
            int iSep2 = s.indexOf('/', iSep1 + 1);
            if(iSep2 < 0) {
              iSep2 = s.indexOf('-', iSep1 + 1);
              // YYYY-MM-DD
              if(iSep2 != 7) iSep1 = -1;
            }
            if(iSep2 > 0) {
              Calendar cal = WUtil.toCalendar(s, null);
              if(cal != null) {
                if(boLTE) cal.add(Calendar.DATE, 1);
                value = toSQL(cal);
              }
            }
          }
        }
      }
      else if(valueTmp instanceof Calendar) {
        Calendar cal = (Calendar) valueTmp;
        if(boLTE) cal.add(Calendar.DATE, 1);
        value = toSQL(cal);
      }
      else if(valueTmp instanceof java.util.Date) {
        if(boLTE) {
          Calendar cal = Calendar.getInstance();
          cal.setTimeInMillis(((java.util.Date) valueTmp).getTime());
          cal.add(Calendar.DATE, 1);
          value = toSQL(cal);
        }
        else {
          value = toSQL((java.util.Date) valueTmp);
        }
      }
      else if(valueTmp instanceof Boolean) {
        Object booleanValue = getBooleanValue(valueTmp);
        if(booleanValue == null) {
          value = "NULL";
        }
        else if(booleanValue instanceof Number) {
          value = booleanValue.toString();
        }
        else {
          value = "'" + booleanValue + "'";
        }
      }
      else if(valueTmp instanceof Number) {
        value = valueTmp.toString();
      }
      else {
        String json = JSON.stringify(valueTmp);
        value = "'" + json.replace("'", "''") + "'";
      }
      
      sbResult.append(key.toUpperCase());
      if(boNE) {
        sbResult.append(" <> ");
      }
      else if(boGT) {
        sbResult.append(" > ");
      }
      else if(boGTE) {
        sbResult.append(" >= ");
      }
      else if(boLT) {
        sbResult.append(" < ");
      }
      else if(boLTE) {
        sbResult.append(" <= ");
      }
      else if(boLike) {
        sbResult.append(" LIKE ");
      }
      else {
        sbResult.append('=');
      }
      sbResult.append(value);
      sbResult.append(" AND ");
    }
    String sResult = sbResult.toString();
    if(sResult.length() > 0) {
      sResult = sResult.substring(0, sResult.length()-5);
    }
    return sResult;
  }
  
  protected
  List<String> getFields(Map<String, ?> mapValues)
  {
    List<String> listResult = new ArrayList<String>();
    if(mapValues == null || mapValues.isEmpty()) {
      return listResult;
    }
    Iterator<String> iterator = mapValues.keySet().iterator();
    while(iterator.hasNext()) {
      listResult.add(iterator.next());
    }
    return listResult;
  }
  
  protected
  boolean useSequence()
  {
    Connection conn = null;
    try {
      return useSequence(conn);
    }
    finally {
      closeConnection(conn);
    }
  }
  
  protected
  boolean useSequence(Connection connection)
  {
    if(_useSequece != null) return _useSequece.booleanValue();
    try {
      DatabaseMetaData bdbmd = connection.getMetaData();
      String sDatabaseProductName = bdbmd.getDatabaseProductName();
      if(sDatabaseProductName != null && sDatabaseProductName.toUpperCase().indexOf("ORACLE") >= 0) {
        _useSequece = Boolean.TRUE;
        _dbms = "oracle";
      }
      else {
        _useSequece = Boolean.FALSE;
        if(sDatabaseProductName != null && sDatabaseProductName.length() > 0) {
          _dbms = sDatabaseProductName.toLowerCase();
        }
      }
    }
    catch(Throwable ex) {
      return false;
    }
    if(_useSequece == null) return false;
    return _useSequece.booleanValue();
  }
  
  protected
  String nextValString()
  {
    byte arrayOfByte[] = new byte[12];
    ByteBuffer bb = ByteBuffer.wrap( arrayOfByte );
    bb.putInt((int)(System.currentTimeMillis() / 1000) );
    bb.putInt( _genmachine );
    bb.putInt( _nextInc.getAndIncrement() );
    final StringBuilder buf = new StringBuilder(24);
    for(final byte b : arrayOfByte) {
      buf.append(String.format("%02x", b & 0xff));
    }
    return buf.toString();
  }
  
  protected
  int nextVal(Connection connection, String sequenceName)
    throws Exception
  {
    int iResult = 0;
    if(useSequence(connection)) {
      Statement stm = null;
      ResultSet rs = null;
      try {
        stm = connection.createStatement();
        rs = stm.executeQuery("SELECT " + sequenceName + ".NEXTVAL FROM DUAL");
        if(rs.next()) iResult = rs.getInt(1);
      }
      finally {
        if(rs  != null) try{ rs.close();  } catch(Exception ex) {}
        if(stm != null) try{ stm.close(); } catch(Exception ex) {}
      }
    }
    else {
      PreparedStatement pstm = null;
      ResultSet rs = null;
      try {
        pstm = connection.prepareStatement("UPDATE " + _sequenceTab + " SET " + _sequenceVal + "=" + _sequenceVal + "+1 WHERE " + _sequenceKey + "=?");
        pstm.setQueryTimeout(_queryTimeout);
        pstm.setString(1, sequenceName);
        int iRows = pstm.executeUpdate();
        pstm.close();
        if(iRows == 1) {
          pstm = connection.prepareStatement("SELECT " + _sequenceVal + " FROM " + _sequenceTab + " WHERE " + _sequenceKey+ "=?");
          pstm.setQueryTimeout(_queryTimeout);
          pstm.setString(1, sequenceName);
          rs = pstm.executeQuery();
          if(rs.next()) iResult = rs.getInt(1);
        }
        else {
          iResult = 1;
          pstm = connection.prepareStatement("INSERT INTO " + _sequenceTab + "(" + _sequenceKey + "," + _sequenceVal + ") VALUES(?,?)");
          pstm.setQueryTimeout(_queryTimeout);
          pstm.setString(1, sequenceName);
          pstm.setInt(2, iResult);
          pstm.executeUpdate();
        }
      }
      finally {
        if(rs   != null) try{ rs.close();   } catch(Exception ex) {}
        if(pstm != null) try{ pstm.close(); } catch(Exception ex) {}
      }
    }
    return iResult;
  }
  
  protected static
  byte[] getBLOBContent(ResultSet rs, int index)
    throws Exception
  {
    Blob blob = rs.getBlob(index);
    if(blob == null) return null;
    
    InputStream is = blob.getBinaryStream();
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    byte[] abDataBuffer = new byte[1024];
    int iBytesRead = 0;
    while((iBytesRead = is.read(abDataBuffer, 0, abDataBuffer.length)) != -1) {
      baos.write(abDataBuffer, 0, iBytesRead);
    }
    baos.flush();
    
    return baos.toByteArray();
  }
  
  protected static
  byte[] getBLOBContent(ResultSet rs, String fieldName)
    throws Exception
  {
    Blob blob = rs.getBlob(fieldName);
    if(blob == null) return null;
    
    InputStream is = blob.getBinaryStream();
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    byte[] abDataBuffer = new byte[1024];
    int iBytesRead = 0;
    while((iBytesRead = is.read(abDataBuffer, 0, abDataBuffer.length)) != -1) {
      baos.write(abDataBuffer, 0, iBytesRead);
    }
    baos.flush();
    
    return baos.toByteArray();
  }
  
  protected
  boolean setEmptyBLOB(Connection connection, String filename)
    throws Exception
  {
    int result = 0;
    String sSQL = "UPDATE " + _filesTable + " SET " + _filesContField + "=" + _filesEmptyFunc + " WHERE " + _filesNameField + "=?";
    PreparedStatement pstm = null;
    try{
      pstm = connection.prepareStatement(sSQL);
      pstm.setQueryTimeout(_queryTimeout);
      pstm.setString(1, filename);
      result = pstm.executeUpdate();
    }
    finally {
      if(pstm != null) try{ pstm.close(); } catch(Exception ex) {}
    }
    return result > 0;
  }
  
  protected
  boolean setBLOBContent(Connection connection, String filename, byte[] abBlobContent)
    throws Exception
  {
    boolean setEmptyRes = setEmptyBLOB(connection, filename);
    if(abBlobContent == null || abBlobContent.length == 0) {
      return setEmptyRes;
    }
    String sSQL = "SELECT " + _filesContField + " FROM " + _filesTable + " WHERE " + _filesNameField + "='" + filename.replace("'", "''") + "' FOR UPDATE";
    boolean boResult = false;
    Statement stm = null;
    ResultSet rs = null;
    try{
      stm = connection.createStatement();
      stm.setQueryTimeout(_queryTimeout);
      rs = stm.executeQuery(sSQL);
      if(rs.next()) {
        Blob blob = rs.getBlob(_filesContField);
        OutputStream blobOutputStream = blob.setBinaryStream(0);
        for(int i = 0; i < abBlobContent.length; i++) {
          blobOutputStream.write(abBlobContent[i]);
        }
        blobOutputStream.flush();
        blobOutputStream.close();
        boResult = true;
      }
      
      if(boResult) {
        if(this._conn != null) {
          try {
            // Required with oracle driver 
            this._conn.commit();
          }
          catch(Exception ex) {
          }
        }
      }
    }
    finally {
      if(rs  != null) try{ rs.close();  } catch(Exception ex) {}
      if(stm != null) try{ stm.close(); } catch(Exception ex) {}
    }
    return boResult;
  }
  
  protected static
  List<Map<String, Object>> toList(ResultSet rs, int limit, int skip)
    throws Exception
  {
    return toList(rs, limit, skip, false);
  }
  
  protected static
  List<Map<String, Object>> toList(ResultSet rs, int limit, int skip, boolean excludeBlobs)
    throws Exception
  {
    List<Map<String, Object>> listResult = new ArrayList<Map<String,Object>>();
    
    ResultSetMetaData rsmd = rs.getMetaData();
    int columnCount = rsmd.getColumnCount();
    
    int rows = 0;
    while(rs.next()) {
      rows++;
      if(skip > 0 && rows <= skip) continue;
      
      Map<String, Object> mapRecord = new HashMap<String, Object>();
      
      for(int i = 1; i <= columnCount; i++) {
        String sField  = rsmd.getColumnName(i);
        int iFieldType = rsmd.getColumnType(i);
        
        if(sField.indexOf("(") >= 0) sField = "value";
        
        if(iFieldType == java.sql.Types.CHAR || iFieldType == java.sql.Types.VARCHAR) {
          mapRecord.put(sField.toLowerCase(), rs.getString(i));
        }
        else if(iFieldType == java.sql.Types.DATE) {
          mapRecord.put(sField.toLowerCase(), rs.getDate(i));
        }
        else if(iFieldType == java.sql.Types.TIME || iFieldType == java.sql.Types.TIMESTAMP) {
          mapRecord.put(sField.toLowerCase(), rs.getTimestamp(i));
        }
        else if(iFieldType == java.sql.Types.BINARY || iFieldType == java.sql.Types.BLOB || iFieldType == java.sql.Types.CLOB) {
          mapRecord.put(sField.toLowerCase(), getBLOBContent(rs, i));
        }
        else {
          String sValue = rs.getString(i);
          if(sValue != null) {
            if(sValue.indexOf('.') >= 0 || sValue.indexOf(',') >= 0) {
              mapRecord.put(sField.toLowerCase(), new Double(rs.getDouble(i)));
            }
            else {
              mapRecord.put(sField.toLowerCase(), new Integer(rs.getInt(i)));
            }
          }
          else {
            mapRecord.put(sField.toLowerCase(), null);
          }
        }
      }
      
      listResult.add(mapRecord);
      
      if(limit > 0 && listResult.size() >= limit) break;
    }
    
    return listResult;
  }

  protected static
  Map<String, Object> toMap(ResultSet rs)
    throws Exception
  {
    return toMap(rs, false);
  }
  
  protected static
  Map<String, Object> toMap(ResultSet rs, boolean excludeBlobs)
    throws Exception
  {
    Map<String, Object> mapResult = new HashMap<String, Object>();
    
    ResultSetMetaData rsmd = rs.getMetaData();
    int iColumnCount = rsmd.getColumnCount();
    
    for(int i = 1; i <= iColumnCount; i++) {
      String sField  = rsmd.getColumnName(i);
      int iFieldType = rsmd.getColumnType(i);
      if(iFieldType == java.sql.Types.CHAR || iFieldType == java.sql.Types.VARCHAR) {
        mapResult.put(sField.toLowerCase(), rs.getString(i));
      }
      else if(iFieldType == java.sql.Types.DATE) {
        mapResult.put(sField.toLowerCase(), rs.getDate(i));
      }
      else if(iFieldType == java.sql.Types.TIME || iFieldType == java.sql.Types.TIMESTAMP) {
        mapResult.put(sField.toLowerCase(), rs.getTimestamp(i));
      }
      else if(iFieldType == java.sql.Types.BINARY || iFieldType == java.sql.Types.BLOB || iFieldType == java.sql.Types.CLOB) {
        mapResult.put(sField.toLowerCase(), getBLOBContent(rs, i));
      }
      else {
        String sValue = rs.getString(i);
        if(sValue != null) {
          if(sValue.indexOf('.') >= 0 || sValue.indexOf(',') >= 0) {
            mapResult.put(sField.toLowerCase(), new Double(rs.getDouble(i)));
          }
          else {
            mapResult.put(sField.toLowerCase(), new Integer(rs.getInt(i)));
          }
        }
        else {
          mapResult.put(sField.toLowerCase(), null);
        }
      }
    }
    
    return mapResult;
  }
  
  protected
  Object getBooleanValue(Object oValue)
  {
    if(oValue == null) return null;
    boolean value = WUtil.toBoolean(oValue, false);
    if(value) {
      return _trueValue;
    }
    else {
      return _falseValue;
    }
  }
  
  protected
  Object getIdValue(String id)
  {
    if(id == null || id.length() == 0) {
      return null;
    }
    if(_idFieldName == null || _idFieldName.length() == 0) {
      return null;
    }
    int iId = WUtil.toInt(id, 0);
    if(iId != 0) {
      return iId;
    }
    return id;
  }
  
  protected
  String toSQL(Object val)
  {
    if(val == null) return "NULL";
    
    if(val instanceof String) {
      return "'" + ((String) val).replace("'", "''") + "'";
    }
    else if(val instanceof Number) {
      return val.toString();
    }
    else if(val instanceof Boolean) {
      Object booleanValue = getBooleanValue(val);
      if(booleanValue instanceof Number) {
        return booleanValue.toString();
      }
      else if(booleanValue instanceof String) {
        return "'" + booleanValue + "'";
      }
      else {
        return WUtil.toBoolean(val, false) ? "1" : "0";
      }
    }
    else if(val instanceof Timestamp) {
      return toSQL((Timestamp) val);
    }
    else if(val instanceof java.util.Date) {
      return toSQL((java.util.Date) val);
    }
    else if(val instanceof java.sql.Date) {
      return toSQL((java.sql.Date) val);
    }
    else if(val instanceof Calendar) {
      return toSQL((Calendar) val);
    }
    
    return "'" + val.toString().replace("'", "''") + "'";
  }
  
  protected
  String toSQL(Timestamp ts)
  {
    if(ts == null) return "NULL";
    
    Calendar cal = Calendar.getInstance();
    cal.setTimeInMillis(ts.getTime());
    
    return toSQL(cal);
  }
  
  protected
  String toSQL(Calendar cal)
  {
    if(cal == null) return "NULL";
    
    int iYear   = cal.get(java.util.Calendar.YEAR);
    int iMonth  = cal.get(java.util.Calendar.MONTH) + 1;
    int iDay    = cal.get(java.util.Calendar.DAY_OF_MONTH);
    int iHour   = cal.get(Calendar.HOUR_OF_DAY);
    int iMinute = cal.get(Calendar.MINUTE);
    int iSecond = cal.get(Calendar.SECOND);
    String sMonth  = iMonth  < 10 ? "0" + iMonth  : String.valueOf(iMonth);
    String sDay    = iDay    < 10 ? "0" + iDay    : String.valueOf(iDay);
    String sHour   = iHour   < 10 ? "0" + iHour   : String.valueOf(iHour);
    String sMinute = iMinute < 10 ? "0" + iMinute : String.valueOf(iMinute);
    String sSecond = iSecond < 10 ? "0" + iSecond : String.valueOf(iSecond);
    if(_dbms != null && _dbms.indexOf("oracle") >= 0) {
      return "TO_DATE('" + iYear + "-" + sMonth + "-" + sDay + " " + sHour + ":" + sMinute + ":" + sSecond + "','YYYY-MM-DD HH24:MI:SS')";
    }
    return "'" + iYear + "-" + sMonth + "-" + sDay + " " + sHour + ":" + sMinute + ":" + sSecond + "'";
  }
  
  protected
  String toSQL(java.sql.Date date)
  {
    if(date == null) return "NULL";
    
    Calendar cal = Calendar.getInstance();
    cal.setTime(date);
    int iYear  = cal.get(Calendar.YEAR);
    int iMonth = cal.get(Calendar.MONTH) + 1;
    int iDay   = cal.get(Calendar.DAY_OF_MONTH);
    String sMonth = iMonth < 10 ? "0" + iMonth : String.valueOf(iMonth);
    String sDay   = iDay   < 10 ? "0" + iDay   : String.valueOf(iDay);
    if(_dbms != null && _dbms.indexOf("oracle") >= 0) {
      return "TO_DATE('" + iYear + "-" + sMonth + "-" + sDay + "','YYYY-MM-DD')";
    }
    return "'" + iYear + "-" + sMonth + "-" + sDay + "'";
  }
  
  protected
  String toSQL(java.util.Date date)
  {
    if(date == null) return "NULL";
    
    Calendar cal = Calendar.getInstance();
    cal.setTime(date);
    int iYear  = cal.get(Calendar.YEAR);
    int iMonth = cal.get(Calendar.MONTH) + 1;
    int iDay   = cal.get(Calendar.DAY_OF_MONTH);
    String sMonth = iMonth < 10 ? "0" + iMonth : String.valueOf(iMonth);
    String sDay   = iDay   < 10 ? "0" + iDay   : String.valueOf(iDay);
    if(_dbms != null && _dbms.indexOf("oracle") >= 0) {
      return "TO_DATE('" + iYear + "-" + sMonth + "-" + sDay + "','YYYY-MM-DD')";
    }
    return "'" + iYear + "-" + sMonth + "-" + sDay + "'";
  }
  
  protected
  Connection getConnection()
    throws Exception 
  {
    if(this._conn != null) return _conn;
    
    if(dataSource == null || dataSource.length() == 0) {
      throw new Exception("No datasource specified");
    }
    
    Context ctx = new InitialContext();
    
    if(dataSource.indexOf('/') >= 0) {
      DataSource ds = (DataSource) ctx.lookup(dataSource);
      if(ds != null) return ds.getConnection();
      throw new Exception("DataSource " + dataSource + " not available.");
    }
    
    try {
      DataSource ds = (DataSource) ctx.lookup(jdbcPath + dataSource);
      if(ds != null) return ds.getConnection();
    }
    catch(Exception ex) {}
    jdbcPath = "java:/";
    try {
      DataSource ds = (DataSource) ctx.lookup(jdbcPath + dataSource);
      if(ds != null) return ds.getConnection();
    }
    catch(Exception ex) {}
    jdbcPath = "java:/jdbc/";
    try {
      DataSource ds = (DataSource) ctx.lookup(jdbcPath + dataSource);
      if(ds != null) return ds.getConnection();
    }
    catch(Exception ex) {}
    jdbcPath = "java:/comp/env/jdbc/";
    try {
      DataSource ds = (DataSource) ctx.lookup(jdbcPath + dataSource);
      if(ds != null) return ds.getConnection();
    }
    catch(Exception ex) {}
    jdbcPath = "jdbc/";
    try {
      DataSource ds = (DataSource) ctx.lookup(jdbcPath + dataSource);
      if(ds != null) return ds.getConnection();
    }
    catch(Exception ex) {}
    
    throw new Exception("DataSource " + dataSource + " not available.");
  }
  
  protected
  void closeConnection(Connection conn)
  {
    if(this._conn != null) return;
    
    if(conn == null) return;
    
    try {
      conn.close();
    }
    catch(Exception ex) {
      ex.printStackTrace();
    }
  }
  
  private static AtomicInteger _nextInc = new AtomicInteger((new java.util.Random()).nextInt());
  private static final int _genmachine;
  static {
    try {
      // build a 2-byte machine piece based on NICs info
      int machinePiece;
      {
        try {
          StringBuilder sb = new StringBuilder();
          Enumeration<NetworkInterface> e = NetworkInterface.getNetworkInterfaces();
          while( e.hasMoreElements() ) {
            NetworkInterface ni = e.nextElement();
            sb.append( ni.toString() );
          }
          machinePiece = sb.toString().hashCode() << 16;
        } catch(Throwable e) {
          machinePiece = (new Random().nextInt()) << 16;
        }
      }
      // add a 2 byte process piece. It must represent not only the JVM but the class loader.
      // Since static var belong to class loader there could be collisions otherwise
      final int processPiece;
      {
        int processId = new java.util.Random().nextInt();
        try {
          processId = java.lang.management.ManagementFactory.getRuntimeMXBean().getName().hashCode();
        }
        catch(Throwable t) {
        }
        ClassLoader loader = NoSQLElasticsearch.class.getClassLoader();
        int loaderId = loader != null ? System.identityHashCode(loader) : 0;
        
        StringBuilder sb = new StringBuilder();
        sb.append(Integer.toHexString(processId));
        sb.append(Integer.toHexString(loaderId));
        processPiece = sb.toString().hashCode() & 0xFFFF;
      }
      _genmachine = machinePiece | processPiece;
    }
    catch(Exception ex) {
      throw new RuntimeException(ex);
    }
  }
}
