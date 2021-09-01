package org.dew.nosql;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;

import java.lang.reflect.Array;

import java.net.NetworkInterface;
import java.net.URL;

import java.nio.ByteBuffer;

import java.security.MessageDigest;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import org.dew.nosql.json.JSON;
import org.dew.nosql.util.Base64Coder;

import org.dew.nosql.util.WUtil;

/**
 * Implementazione mock di INoSQLDB.
 */
@SuppressWarnings({ "rawtypes", "unchecked" })
public
class NoSQLMock implements INoSQLDB
{
  protected static String logprefix = NoSQLMock.class.getSimpleName() + ".";
  
  protected String  dbname;
  protected boolean debug  = false;
  
  protected static boolean firstload = false;
  protected static String  defaultDatabase = "default";
  protected static Map<String, Map<String, List<Map<String, Object>>>> data = new HashMap<String, Map<String, List<Map<String, Object>>>>();
  static {
    if(!firstload) {
      firstload = true;
      String filePath = null;
      try {
        String sUrl = NoSQLDataSource.getProperty("nosqldb.uri");
        if(sUrl == null || sUrl.length() == 0) {
          sUrl = NoSQLDataSource.getProperty("nosqldb.url");
        }
        if(sUrl != null && sUrl.length() > 0) {
          if(sUrl.startsWith("file:///")) {
            if(sUrl.indexOf(':', 8) >= 0) {
              filePath = sUrl.substring(8);
            }
            else {
              filePath = sUrl.substring(7);
            }
          }
          else {
            filePath = sUrl;
          }
        }
        loadFile(filePath);
      }
      catch(Exception ex) {
        System.err.println("NoSQLMock loadFile(" + filePath + "): " + ex);
      }
    }
  }
  
  public NoSQLMock()
  {
    dbname = NoSQLDataSource.getProperty("nosqldb.dbname",  NoSQLDataSource.getProperty("nosqldb.dbauth"));
    if(dbname == null || dbname.length() == 0) {
      dbname = defaultDatabase;
    }
  }
  
  public NoSQLMock(boolean debug)
  {
    this();
    this.debug = debug;
  }
  
  public NoSQLMock(String dbname)
  {
    if(dbname != null && dbname.length() > 0) {
      this.dbname = dbname;
    }
    else {
      this.dbname = NoSQLDataSource.getProperty("nosqldb.dbname",  NoSQLDataSource.getProperty("nosqldb.dbauth"));
    }
    if(this.dbname == null || this.dbname.length() == 0) {
      this.dbname = defaultDatabase;
    }
  }
  
  public NoSQLMock(String dbname, boolean debug)
  {
    this(dbname);
    this.debug = debug;
  }
  
  @Override
  public
  void setDebug(boolean debug)
  {
    this.debug = debug;
  }
  
  @Override
  public
  boolean isDebug()
  {
    return debug;
  }
  
  @Override
  public 
  Map<String, Object> load(Map<String, Object> mapOptions)
    throws Exception
  {
    if(debug) System.out.println(logprefix + "load(" + mapOptions + ")...");
    
    // Check data file
    String filePath = "";
    if(mapOptions != null) {
      String sFileName = WUtil.toString(mapOptions.get(FILE_NAME), null);
      if(sFileName != null && sFileName.equals("-")) {
        // Clear data
        if(data == null) data = new HashMap<String, Map<String,List<Map<String, Object>>>>();
        data.clear();
        Map<String, Object> mapResult = getInfo();
        if(mapResult != null) {
          mapResult.put("databases", 0);
          mapResult.put("collections", 0);
          mapResult.put("records", 0);
        }
        if(debug) System.out.println(logprefix + "load(" + mapOptions + ") -> " + mapResult);
        return mapResult;
      }
      if(sFileName != null && sFileName.length() > 0) {
        char c0 = sFileName.charAt(0);
        char c1 = sFileName.charAt(1);
        if(c0 == '/' || c1 == ':') {
          filePath = sFileName;
        }
        else {
          filePath = System.getProperty("user.home") + File.separator + sFileName;
        }
      }
    }
    
    int[] counts = loadFile(filePath);
    
    if(counts == null || counts.length < 3 || counts[0] == 0) {
      if(debug) System.out.println(logprefix + "load(" + mapOptions + ") no data available.");
      Map<String, Object> mapResult = getInfo();
      if(mapResult != null) {
        mapResult.put("databases", 0);
        mapResult.put("collections", 0);
        mapResult.put("records", 0);
      }
      if(debug) System.out.println(logprefix + "load(" + mapOptions + ") -> " + mapResult);
      return mapResult;
    }
    
    if(debug) System.out.println(logprefix + "load(" + mapOptions + ") imported databases: " + counts[0] + ", collections: " + counts[1] + ", records: " + counts[2]);
    
    Map<String, Object> mapResult = getInfo();
    if(mapResult != null) {
      mapResult.put("databases", counts[0]);
      mapResult.put("collections", counts[1]);
      mapResult.put("records", counts[2]);
    }
    
    if(debug) System.out.println(logprefix + "load(" + mapOptions + ") -> " + mapResult);
    return mapResult;
  }
  
  @Override
  public 
  boolean save(Map<String, Object> mapOptions)
    throws Exception
  {
    if(debug) System.out.println(logprefix + "save(" + mapOptions + ")...");
    
    boolean result = false;
    
    if(data == null) data = new HashMap<String, Map<String,List<Map<String, Object>>>>();
    
    // Check data file
    String filePath = "";
    if(mapOptions != null) {
      String sFileName = WUtil.toString(mapOptions.get(FILE_NAME), null);
      if(sFileName != null && sFileName.length() > 1) {
        char c0 = sFileName.charAt(0);
        char c1 = sFileName.charAt(1);
        if(c0 == '/' || c1 == ':') {
          filePath = sFileName;
        }
        else {
          filePath = System.getProperty("user.home") + File.separator + sFileName;
        }
      }
    }
    if(filePath == null || filePath.length() == 0) {
      filePath = System.getProperty("user.home") + File.separator + "nosqlmock.json";
    }
    filePath = filePath.replace("$HOME", System.getProperty("user.home"));
    
    // Write File
    try {
      String jsonData = JSON.stringify(data);
      
      saveContent(new String(jsonData).getBytes(), filePath);
      
      result = true;
    }
    catch(Exception ex) {
      if(debug) System.out.println(logprefix + "save(" + mapOptions + "): " + ex);
      result = false;
    }
    
    if(debug) System.out.println(logprefix + "save(" + mapOptions + ") -> " + result);
    return result;
  }
  
  @Override
  public
  Map<String, Object> getInfo()
      throws Exception
  {
    if(debug) System.out.println(logprefix + "getInfo()...");
    
    if(data == null) data = new HashMap<String, Map<String,List<Map<String, Object>>>>();
    
    Map<String, Object> mapResult = new HashMap<String, Object>(2);
    mapResult.put("name",    "Mock");
    mapResult.put("version", "1.0.0");
    
    if(debug) System.out.println(logprefix + "getInfo() -> " + mapResult);
    return mapResult;
  }
  
  @Override
  public
  List<String> getCollections()
      throws Exception
  {
    if(debug) System.out.println(logprefix + "getCollections()...");
    
    if(data == null) data = new HashMap<String, Map<String,List<Map<String, Object>>>>();
    
    List<String> listResult = new ArrayList<String>();
    
    Map<String,List<Map<String, Object>>> mapColData = data.get(dbname);
    if(mapColData != null) {
      Iterator<String> iterator = mapColData.keySet().iterator();
      while(iterator.hasNext()) {
        listResult.add(iterator.next());
      }
    }
    Collections.sort(listResult);
    
    if(debug) System.out.println(logprefix + "getCollections() -> " + listResult);
    return listResult;
  }
  
  @Override
  public 
  boolean drop(String collection) 
      throws Exception
  {
    if(debug) System.out.println(logprefix + "drop(" + collection + ")...");
    boolean result = false;
    
    if(data == null) data = new HashMap<String, Map<String,List<Map<String, Object>>>>();
    
    if(collection != null && collection.length() > 0) {
      Map<String,List<Map<String, Object>>> mapColData = data.get(dbname);
      if(mapColData != null) {
        List<Map<String, Object>> listColData = mapColData.remove(collection);
        result = listColData != null;
      }
    }
    
    if(debug) System.out.println(logprefix + "drop(" + collection + ") -> " + result);
    return result;
  }
  
  @Override
  public
  String insert(String collection, Map<String, ?> mapData)
      throws Exception
  {
    if(debug) System.out.println(logprefix + "insert(" + collection + "," + mapData + ")...");
    
    String result = generateId();
    
    List<Map<String, Object>> listColData = getCollectionData(dbname, collection, true);
    
    Map<String, Object> mapItem = mapObject(mapData);
    mapItem.put("_id", result);
    listColData.add(mapItem);
    
    if(debug) System.out.println(logprefix + "insert(" + collection + "," + mapData + ") -> " + result);
    return result;
  }
  
  @Override
  public
  String insert(String collection, Map<String, ?> mapData, boolean refresh)
      throws Exception
  {
    if(debug) System.out.println(logprefix + "insert(" + collection + "," + mapData + "," + refresh + ")...");
    
    String result = generateId();
    
    List<Map<String, Object>> listColData = getCollectionData(dbname, collection, true);
    
    Map<String, Object> mapItem = mapObject(mapData);
    mapItem.put("_id", result);
    listColData.add(mapItem);
    
    if(debug) System.out.println(logprefix + "insert(" + collection + "," + mapData + "," + refresh + ") -> " + result);
    return result;
  }
  
  @Override
  public
  int bulkIns(String collection, List<Map<String, ?>> listData)
      throws Exception
  {
    if(debug) {
      if(listData != null) {
        System.out.println(logprefix + "bulkIns(" + collection + ", " + listData.size() + " documents)...");
      }
      else {
        System.out.println(logprefix + "bulkIns(" + collection + ", null)...");
      }
    }
    if(listData == null || listData.size() == 0) {
      if(listData != null) {
        System.out.println(logprefix + "bulkIns(" + collection + ", " + listData.size() + " documents) -> 0");
      }
      else {
        System.out.println(logprefix + "bulkIns(" + collection + ", null) -> 0");
      }
      return 0;
    }
    
    List<Map<String, Object>> listColData = getCollectionData(dbname, collection, true);
    
    int countIns = 0;
    for(int i = 0; i < listData.size(); i++) {
      Map<String, ?> mapData = listData.get(i);
      if(mapData == null || mapData.isEmpty()) continue;
      
      Map<String, Object> mapItem = mapObject(mapData);
      mapItem.put("_id", generateId());
      listColData.add(mapItem);
      
      countIns++;
    }
    
    System.out.println(logprefix + "bulkIns(" + collection + ", " + listData.size() + " documents) -> " + countIns);
    return countIns;
  }
  
  @Override
  public
  boolean replace(String collection, Map<String, ?> mapData, String id)
      throws Exception
  {
    if(debug) System.out.println(logprefix + "replace(" + collection + "," + mapData + ")...");
    
    List<Map<String, Object>> listColData = getCollectionData(dbname, collection, false);
    
    boolean result = false;
    for(int i = 0; i < listColData.size(); i++) {
      Map<String, Object> mapItem = listColData.get(i);
      if(mapItem == null) continue;
      String sId = (String) mapItem.get("_id");
      if(sId != null && sId.equals(id)) {
        mapItem.clear();
        mapItem.putAll(mapData);
        result = true;
        break;
      }
    }
    
    if(debug) System.out.println(logprefix + "replace(" + collection + "," + mapData + ") -> " + result);
    return result;
  }
  
  @Override
  public
  int update(String collection, Map<String, ?> mapData, String id)
      throws Exception
  {
    if(debug) System.out.println(logprefix + "update(" + collection + "," + mapData + "," + id + ")...");
    
    if(mapData == null || mapData.isEmpty()) {
      if(debug) System.out.println(logprefix + "update(" + collection + "," + mapData + "," + id + ") -> 0");
      return 0;
    }
    
    List<Map<String, Object>> listColData = getCollectionData(dbname, collection, false);
    
    int result = 0;
    for(int i = 0; i < listColData.size(); i++) {
      Map<String, Object> mapItem = listColData.get(i);
      if(mapItem == null) continue;
      String sId = (String) mapItem.get("_id");
      if(sId != null && sId.equals(id)) {
        mapItem.putAll(mapData);
        result = 1;
        break;
      }
    }
    
    if(debug) System.out.println(logprefix + "update(" + collection + "," + mapData + "," + id + ") -> " + result);
    return result;
  }
  
  @Override
  public
  int update(String collection, Map<String, ?> mapData, Map<String, ?> mapFilter)
      throws Exception
  {
    if(debug) System.out.println(logprefix + "update(" + collection + "," + mapData + "," + mapFilter + ")...");
    
    if(mapData == null || mapData.isEmpty()) {
      if(debug) System.out.println(logprefix + "update(" + collection + "," + mapData + "," + mapFilter + ") -> 0");
      return 0;
    }
    
    int countUpd = 0;
    
    List<Map<String, Object>> listColData = getCollectionData(dbname, collection, false);
    
    for(int i = 0; i < listColData.size(); i++) {
      Map<String, Object> mapItem = listColData.get(i);
      if(mapItem == null) continue;
      if(match(mapItem, mapFilter)) {
        mapItem.putAll(mapData);
        countUpd++;
        break;
      }
    }
    
    if(debug) System.out.println(logprefix + "update(" + collection + "," + mapData + "," + mapFilter + ") -> " + countUpd);
    return countUpd;
  }
  
  @Override
  public
  String upsert(String collection, Map<String, ?> mapData, Map<String, ?> mapFilter)
      throws Exception
  {
    if(debug) System.out.println(logprefix + "upsert(" + collection + "," + mapData + "," + mapFilter + ")...");
    
    String result = null;
    
    List<Map<String, Object>> listColData = getCollectionData(dbname, collection, true);
    
    for(int i = 0; i < listColData.size(); i++) {
      Map<String, Object> mapItem = listColData.get(i);
      if(mapItem == null) continue;
      if(match(mapItem, mapFilter)) {
        mapItem.putAll(mapData);
        result = WUtil.toString(mapItem.get("_id"), null);
        break;
      }
    }
    
    if(result == null || result.length() == 0) {
      result = generateId();
      Map<String, Object> mapItem = mapObject(mapData);
      mapItem.put("_id", result);
      listColData.add(mapItem);
    }
    
    if(debug) System.out.println(logprefix + "upsert(" + collection + "," + mapData + "," + mapFilter + ") -> " + result);
    return result;
  }
  
  @Override
  public
  int unset(String collection, String fields, String id)
      throws Exception
  {
    if(debug) System.out.println(logprefix + "unset(" + collection + "," + fields + "," + id + ")...");
    
    if(fields == null || fields.length() == 0) return 0;
    
    List<Map<String, Object>> listColData = getCollectionData(dbname, collection, false);
    
    int result = 0;
    for(int i = 0; i < listColData.size(); i++) {
      Map<String, Object> mapItem = listColData.get(i);
      if(mapItem == null) continue;
      String sId = (String) mapItem.get("_id");
      if(sId != null && sId.equals(id)) {
        List<String> listFields = WUtil.stringToList(fields);
        if(listFields != null && listFields.size() > 0) {
          for(int f = 0; f < listFields.size(); f++) {
            mapItem.remove(listFields.get(f));
          }
        }
        result = 1;
        break;
      }
    }
    
    if(debug) System.out.println(logprefix + "unset(" + collection + "," + fields + "," + id + ") -> " + result);
    return result;
  }
  
  @Override
  public
  int inc(String collection, String id, String field, Number value)
      throws Exception
  {
    if(debug) System.out.println(logprefix + "inc(" + collection + "," + id + "," + field + "," + value + ")...");
    
    List<Map<String, Object>> listColData = getCollectionData(dbname, collection, false);
    
    int result = 0;
    for(int i = 0; i < listColData.size(); i++) {
      Map<String, Object> mapItem = listColData.get(i);
      if(mapItem == null) continue;
      String sId = (String) mapItem.get("_id");
      if(sId != null && sId.equals(id)) {
        if(value == null) value = new Integer(0);
        
        if(field != null && field.length() > 0) {
          Number prevValue = WUtil.toNumber(mapItem.get(field), null);
          if(prevValue instanceof Double) {
            mapItem.put(field, new Double(prevValue.doubleValue() + value.doubleValue()));
          }
          else if(prevValue instanceof Long) {
            mapItem.put(field, new Long(prevValue.longValue() + value.longValue()));
          }
          else if(prevValue != null) {
            mapItem.put(field, new Integer(prevValue.intValue() + value.intValue()));
          }
          else {
            mapItem.put(field, value);
          }
          result = 1;
        }
        
        break;
      }
    }
    
    if(debug) System.out.println(logprefix + "inc(" + collection + "," + id + "," + field + "," + value + ") -> " + result);
    return result;
  }
  
  @Override
  public
  int inc(String collection, String id, String field1, Number value1, String field2, Number value2)
      throws Exception
  {
    if(debug) System.out.println(logprefix + "inc(" + collection + "," + id + "," + field1 + "," + value1 + "," + field2 + "," + value2 + ")...");
    
    List<Map<String, Object>> listColData = getCollectionData(dbname, collection, false);
    
    int result = 0;
    for(int i = 0; i < listColData.size(); i++) {
      Map<String, Object> mapItem = listColData.get(i);
      if(mapItem == null) continue;
      String sId = (String) mapItem.get("_id");
      if(sId != null && sId.equals(id)) {
        if(value1 == null) value1 = new Integer(0);
        if(value2 == null) value2 = new Integer(0);
        
        if(field1 != null && field1.length() > 0) {
          Number prevValue = WUtil.toNumber(mapItem.get(field1), null);
          if(prevValue instanceof Double) {
            mapItem.put(field1, new Double(prevValue.doubleValue() + value1.doubleValue()));
          }
          else if(prevValue instanceof Long) {
            mapItem.put(field1, new Long(prevValue.longValue() + value1.longValue()));
          }
          else if(prevValue != null) {
            mapItem.put(field1, new Integer(prevValue.intValue() + value1.intValue()));
          }
          else {
            mapItem.put(field1, value1);
          }
          result = 1;
        }
        if(field2 != null && field2.length() > 0) {
          Number prevValue = WUtil.toNumber(mapItem.get(field1), null);
          if(prevValue instanceof Double) {
            mapItem.put(field2, new Double(prevValue.doubleValue() + value2.doubleValue()));
          }
          else if(prevValue instanceof Long) {
            mapItem.put(field2, new Long(prevValue.longValue() + value2.longValue()));
          }
          else if(prevValue != null) {
            mapItem.put(field2, new Integer(prevValue.intValue() + value2.intValue()));
          }
          else {
            mapItem.put(field2, value1);
          }
          result = 1;
        }
        
        break;
      }
    }
    
    if(debug) System.out.println(logprefix + "inc(" + collection + "," + id + "," + field1 + "," + value1 + "," + field2 + "," + value2 + ") -> " + result);
    return result;
  }
  
  @Override
  public
  int inc(String collection, Map<String, ?> mapFilter, String field, Number value)
      throws Exception
  {
    if(debug) System.out.println(logprefix + "inc(" + collection + "," + mapFilter + "," + field + "," + value + ")...");
    
    int countUpd = 0;
    
    List<Map<String, Object>> listColData = getCollectionData(dbname, collection, false);
    
    for(int i = 0; i < listColData.size(); i++) {
      Map<String, Object> mapItem = listColData.get(i);
      if(mapItem == null) continue;
      if(match(mapItem, mapFilter)) {
        if(value == null) value = new Integer(0);
        if(field != null && field.length() > 0) {
          Number prevValue = WUtil.toNumber(mapItem.get(field), null);
          if(prevValue instanceof Double) {
            mapItem.put(field, new Double(prevValue.doubleValue() + value.doubleValue()));
          }
          else if(prevValue instanceof Long) {
            mapItem.put(field, new Long(prevValue.longValue() + value.longValue()));
          }
          else if(prevValue != null) {
            mapItem.put(field, new Integer(prevValue.intValue() + value.intValue()));
          }
          else {
            mapItem.put(field, value);
          }
          countUpd += 1;
        }
        break;
      }
    }
    
    if(debug) System.out.println(logprefix + "inc(" + collection + "," + mapFilter + "," + field + "," + value + ") -> " + countUpd);
    return countUpd;
  }
  
  @Override
  public
  int inc(String collection, Map<String, ?> mapFilter, String field1, Number value1, String field2, Number value2)
      throws Exception
  {
    if(debug) System.out.println(logprefix + "inc(" + collection + "," + mapFilter + "," + field1 + "," + value1 + "," + field2 + "," + value2 + ")...");
    
    int countUpd = 0;
    
    List<Map<String, Object>> listColData = getCollectionData(dbname, collection, false);
    
    for(int i = 0; i < listColData.size(); i++) {
      Map<String, Object> mapItem = listColData.get(i);
      if(mapItem == null) continue;
      if(match(mapItem, mapFilter)) {
        if(value1 == null) value1 = new Integer(0);
        if(value2 == null) value2 = new Integer(0);
        
        if(field1 != null && field1.length() > 0) {
          Number prevValue = WUtil.toNumber(mapItem.get(field1), null);
          if(prevValue instanceof Double) {
            mapItem.put(field1, new Double(prevValue.doubleValue() + value1.doubleValue()));
          }
          else if(prevValue instanceof Long) {
            mapItem.put(field1, new Long(prevValue.longValue() + value1.longValue()));
          }
          else if(prevValue != null) {
            mapItem.put(field1, new Integer(prevValue.intValue() + value1.intValue()));
          }
          else {
            mapItem.put(field1, value1);
          }
          countUpd += 1;
        }
        if(field2 != null && field2.length() > 0) {
          Number prevValue = WUtil.toNumber(mapItem.get(field1), null);
          if(prevValue instanceof Double) {
            mapItem.put(field2, new Double(prevValue.doubleValue() + value2.doubleValue()));
          }
          else if(prevValue instanceof Long) {
            mapItem.put(field2, new Long(prevValue.longValue() + value2.longValue()));
          }
          else if(prevValue != null) {
            mapItem.put(field2, new Integer(prevValue.intValue() + value2.intValue()));
          }
          else {
            mapItem.put(field2, value1);
          }
          countUpd += 1;
        }
        
        break;
      }
    }
    
    if(debug) System.out.println(logprefix + "inc(" + collection + "," + mapFilter + "," + field1 + "," + value1 + "," + field2 + "," + value2 + ") -> " + countUpd);
    return countUpd;
  }
  
  @Override
  public
  int delete(String collection, String id)
      throws Exception
  {
    if(debug) System.out.println(logprefix + "delete(" + collection + "," + id + ")...");
    
    if(collection == null || id == null) {
      if(debug) System.out.println(logprefix + "delete(" + collection + "," + id + ") -> -1");
      return -1;
    }
    
    List<Map<String, Object>> listColData = getCollectionData(dbname, collection, false);
    
    int iIndexOf = -1;
    for(int i = 0; i < listColData.size(); i++) {
      Map<String, Object> mapItem = listColData.get(i);
      if(mapItem == null) continue;
      String sId = (String) mapItem.get("_id");
      if(sId != null && sId.equals(id)) {
        iIndexOf = i;
        break;
      }
    }
    
    int iResult = 0;
    if(iIndexOf >= 0) {
      listColData.remove(iIndexOf);
      iResult = 1;
    }
    
    if(debug) System.out.println(logprefix + "delete(" + collection + "," + id + ") -> " + iResult);
    return iResult;
  }
  
  @Override
  public
  int delete(String collection, Map<String, ?> mapFilter)
      throws Exception
  {
    if(debug) System.out.println(logprefix + "delete(" + collection + "," + mapFilter + ")...");
    
    if(collection == null || mapFilter == null) {
      if(debug) System.out.println(logprefix + "delete(" + collection + "," + mapFilter + ") -> -1");
      return -1;
    }
    
    int iResult = 0;
    
    List<Map<String, Object>> listColData = getCollectionData(dbname, collection, false);
    
    Iterator<Map<String, Object>> iterator = listColData.iterator();
    while(iterator.hasNext()) {
      Map<String, Object> mapItem = iterator.next();
      if(mapItem == null) continue;
      if(match(mapItem, mapFilter)) {
        iterator.remove();
        iResult++;
        break;
      }
    }
    
    if(debug) System.out.println(logprefix + "delete(" + collection + "," + mapFilter + ") -> " + iResult);
    return iResult;
  }
  
  @Override
  public
  List<Map<String, Object>> find(String collection, Map<String, ?> mapFilter, String fields)
      throws Exception
  {
    if(debug) System.out.println(logprefix + "find(" + collection + "," + mapFilter + ",\"" + fields + "\")...");
    
    List<Map<String, Object>> listResult = new ArrayList<Map<String, Object>>();
    
    List<Map<String, Object>> listColData = getCollectionData(dbname, collection, false);
    
    Iterator<Map<String, Object>> iterator = listColData.iterator();
    while(iterator.hasNext()) {
      Map<String, Object> mapItem = iterator.next();
      if(mapItem == null) continue;
      if(match(mapItem, mapFilter)) {
        listResult.add(mapItem);
      }
    }
    
    if(debug) System.out.println(logprefix + "find(" + collection + "," + mapFilter + ",\"" + fields + "\") -> " + listResult.size() + " documents");
    return listResult;
  }
  
  @Override
  public
  List<Map<String, Object>> find(String collection, Map<String, ?> mapFilter, String fields, String orderBy, int limit)
      throws Exception
  {
    if(debug) System.out.println(logprefix + "find(" + collection + "," + mapFilter + ",\"" + fields + "\",\"" + orderBy + "\"," + limit + ")...");
    
    List<Map<String, Object>> listResult = new ArrayList<Map<String, Object>>();
    
    List<Map<String, Object>> listColData = getCollectionData(dbname, collection, false);
    
    Iterator<Map<String, Object>> iterator = listColData.iterator();
    while(iterator.hasNext()) {
      Map<String, Object> mapItem = iterator.next();
      if(mapItem == null) continue;
      if(match(mapItem, mapFilter)) {
        listResult.add(mapItem);
        if(limit > 0 && listResult.size() >= limit) {
          break;
        }
      }
    }
    
    if(orderBy != null && orderBy.length() > 0) {
      String key = null;
      int sep = orderBy.indexOf(',');
      key = sep > 0 ? orderBy.substring(0, sep) : orderBy;
      String type  = getOrderType(key);
      boolean desc = type != null && type.equals(":desc");
      key = getOrderField(key);
      int iFirst = 0;
      int iLast  = listResult.size() - 1;
      boolean boSorted = true;
      do {
        for(int i = iLast; i > iFirst; i--) {
          Map<String, Object> m1 = listResult.get(i);
          Map<String, Object> m2 = listResult.get(i - 1);
          Object o1  = m1.get(key);
          Object o2  = m2.get(key);
          boolean lt = false;
          if(o1 instanceof Comparable && o2 instanceof Comparable) {
            lt = ((Comparable) o1).compareTo(o2) < 0;
          }
          else {
            lt = o1 == null && o2 != null;
          }
          if(lt) {
            listResult.set(i,   m2);
            listResult.set(i-1, m1);
            boSorted = false;
          }
        }
        iFirst++;
      }
      while((iLast > iFirst) &&(!boSorted));
      if(desc) {
        listResult = reverse(listResult);
      }
    }
    
    if(debug) System.out.println(logprefix + "find(" + collection + "," + mapFilter + ",\"" + fields + "\",\"" + orderBy + "\"," + limit + ") -> " + listResult.size() + " documents");
    return listResult;
  }
  
  @Override
  public
  List<Map<String, Object>> find(String collection, Map<String, ?> mapFilter, String fields, String orderBy, int limit, int skip)
    throws Exception
  {
    if(debug) System.out.println(logprefix + "find(" + collection + "," + mapFilter + ",\"" + fields + "\",\"" + orderBy + "\"," + limit + "," + skip + ")...");
    
    List<Map<String, Object>> listResult = new ArrayList<Map<String, Object>>();
    
    List<Map<String, Object>> listColData = getCollectionData(dbname, collection, false);
    
    int rows = 0;
    Iterator<Map<String, Object>> iterator = listColData.iterator();
    while(iterator.hasNext()) {
      Map<String, Object> mapItem = iterator.next();
      if(mapItem == null) continue;
      if(match(mapItem, mapFilter)) {
        rows++;
        if(rows <= skip) continue;
        listResult.add(mapItem);
        if(limit > 0 && listResult.size() >= limit) {
          break;
        }
      }
    }
    
    if(orderBy != null && orderBy.length() > 0) {
      String key = null;
      int sep = orderBy.indexOf(',');
      key = sep > 0 ? orderBy.substring(0, sep) : orderBy;
      String type  = getOrderType(key);
      boolean desc = type != null && type.equals(":desc");
      key = getOrderField(key);
      int iFirst = 0;
      int iLast  = listResult.size() - 1;
      boolean boSorted = true;
      do {
        for(int i = iLast; i > iFirst; i--) {
          Map<String, Object> m1 = listResult.get(i);
          Map<String, Object> m2 = listResult.get(i - 1);
          Object o1  = m1.get(key);
          Object o2  = m2.get(key);
          boolean lt = false;
          if(o1 instanceof Comparable && o2 instanceof Comparable) {
            lt = ((Comparable) o1).compareTo(o2) < 0;
          }
          else {
            lt = o1 == null && o2 != null;
          }
          if(lt) {
            listResult.set(i,   m2);
            listResult.set(i-1, m1);
            boSorted = false;
          }
        }
        iFirst++;
      }
      while((iLast > iFirst) &&(!boSorted));
      if(desc) {
        listResult = reverse(listResult);
      }
    }
    
    if(debug) System.out.println(logprefix + "find(" + collection + "," + mapFilter + ",\"" + fields + "\",\"" + orderBy + "\"," + limit + "," + skip + ") -> " + listResult.size() + " documents");
    return listResult;
  }
  
  @Override
  public
  List<Map<String, Object>> search(String collection, String field, String text)
      throws Exception
  {
    if(debug) System.out.println(logprefix + "search(" + collection + "," + field + "," + text + ")...");
    
    List<Map<String, Object>> listResult = new ArrayList<Map<String, Object>>();
    
    List<Map<String, Object>> listColData = getCollectionData(dbname, collection, false);
    
    Iterator<Map<String, Object>> iterator = listColData.iterator();
    while(iterator.hasNext()) {
      Map<String, Object> mapItem = iterator.next();
      if(mapItem == null) continue;
      if(match(mapItem, field, text)) {
        listResult.add(mapItem);
      }
    }
    
    if(debug) System.out.println(logprefix + "search(" + collection + "," + field + "," + text + ") -> " + listResult.size() + " documents");
    return listResult;
  }
  
  @Override
  public
  List<Map<String, Object>> group(String collection, Map<String, ?> mapFilter, String field, String groupFunction)
      throws Exception
  {
    if(debug) System.out.println(logprefix + "group(" + collection + "," + mapFilter + ",\"" + field + "\",\"" + groupFunction + "\")...");
    
    List<Map<String, Object>> listResult = new ArrayList<Map<String, Object>>();
    
    if(groupFunction == null || groupFunction.length() == 0) {
      return listResult;
    }
    String groupFunctionLC = groupFunction.toLowerCase();
    boolean count = groupFunctionLC.startsWith("count");
    boolean sum   = groupFunctionLC.startsWith("sum");
    boolean avg   = groupFunctionLC.startsWith("avg");
    boolean min   = groupFunctionLC.startsWith("min");
    boolean max   = groupFunctionLC.startsWith("max");
    
    String field2 = null;
    int iSep = groupFunction.indexOf('(');
    if(iSep > 0) {
      field2 = groupFunction.substring(iSep + 1);
      iSep = field2.indexOf(')');
      if(iSep >= 0) {
        field2 = field2.substring(0, iSep);
      }
    }
    if(field2 == null || field2.length() == 0 || field2.equals("*")) {
      field2 = field;
    }
    
    Map<Object,Object>  mapGroupBy = new HashMap<Object, Object>();
    Map<Object,Integer> mapCounts  = new HashMap<Object, Integer>();
    
    List<Map<String, Object>> listColData = getCollectionData(dbname, collection, false);
    
    Iterator<Map<String, Object>> iterator = listColData.iterator();
    while(iterator.hasNext()) {
      Map<String, Object> mapItem = iterator.next();
      if(mapItem == null) continue;
      if(match(mapItem, mapFilter)) {
        Object val = mapItem.get(field);
        if(val == null) val = "null";
        if(count) {
          int iPrev = WUtil.toInt(mapGroupBy.get(val), 0);
          int iCurr = iPrev + 1;
          mapGroupBy.put(val, iCurr);
        }
        else if(sum) {
          Object val2 = mapItem.get(field2);
          if(val2 instanceof Integer) {
            int iPrev = WUtil.toInt(mapGroupBy.get(val), 0);
            int iCurr = iPrev + ((Number) val2).intValue();
            mapGroupBy.put(val, iCurr);
          }
          else if(val2 instanceof Long) {
            long lPrev = WUtil.toLong(mapGroupBy.get(val), 0l);
            long lCurr = lPrev + ((Number) val2).longValue();
            mapGroupBy.put(val, lCurr);
          }
          else if(val2 instanceof Double) {
            double dPrev = WUtil.toDouble(mapGroupBy.get(val), 0l);
            double dCurr = dPrev + ((Number) val2).doubleValue();
            mapGroupBy.put(val, dCurr);
          }
          else {
            int iPrev = WUtil.toInt(mapGroupBy.get(val), 0);
            int iCurr = iPrev + WUtil.toInt(val2, 0);
            mapGroupBy.put(val, iCurr);
          }
        }
        else if(avg) {
          Object val2 = mapItem.get(field2);
          if(val2 instanceof Integer) {
            int iPrev = WUtil.toInt(mapGroupBy.get(val), 0);
            int iCurr = iPrev + ((Number) val2).intValue();
            mapGroupBy.put(val, iCurr);
          }
          else if(val2 instanceof Long) {
            long lPrev = WUtil.toLong(mapGroupBy.get(val), 0l);
            long lCurr = lPrev + ((Number) val2).longValue();
            mapGroupBy.put(val, lCurr);
          }
          else if(val2 instanceof Double) {
            double dPrev = WUtil.toDouble(mapGroupBy.get(val), 0l);
            double dCurr = dPrev + ((Number) val2).doubleValue();
            mapGroupBy.put(val, dCurr);
          }
          else {
            int iPrev = WUtil.toInt(mapGroupBy.get(val), 0);
            int iCurr = iPrev + WUtil.toInt(val2, 0);
            mapGroupBy.put(val, iCurr);
          }
        }
        else if(min) {
          Object val2 = mapItem.get(field2);
          Object prev = mapGroupBy.get(val);
          if(prev instanceof Comparable && val2 instanceof Comparable) {
            if(((Comparable) val2).compareTo(prev) == -1) {
              mapGroupBy.put(val, val2);
            }
          }
        }
        else if(max) {
          Object val2 = mapItem.get(field2);
          Object prev = mapGroupBy.get(val);
          if(prev instanceof Comparable && val2 instanceof Comparable) {
            if(((Comparable) val2).compareTo(prev) == 1) {
              mapGroupBy.put(val, val2);
            }
          }
        }
        else {
          int iPrev = WUtil.toInt(mapGroupBy.get(val), 0);
          int iCurr = iPrev + 1;
          mapGroupBy.put(val, iCurr);
        }
        
        int iPrevCount = WUtil.toInt(mapCounts.get(val), 0);
        int iCurrCount = iPrevCount + 1;
        mapCounts.put(val, iCurrCount);
      }
    }
    
    Iterator<Map.Entry<Object, Object>> groupIterator = mapGroupBy.entrySet().iterator();
    while(groupIterator.hasNext()) {
      Map.Entry<Object, Object> entry = groupIterator.next();
      Object key = entry.getKey();
      Object val = entry.getValue();
      
      if(avg) {
        Integer groupCount = mapCounts.get(key);
        if(groupCount != null && groupCount.intValue() != 0) {
          if(val instanceof Integer) {
            val = ((Number) val).intValue() / groupCount.intValue();
          }
          else if(val instanceof Long) {
            val = ((Number) val).longValue() / groupCount.intValue();
          }
          else if(val instanceof Double) {
            val = ((Number) val).doubleValue() / groupCount.intValue();
          }
          else {
            val = ((Number) val).intValue() / groupCount.intValue();
          }
        }
      }
      
      Map<String, Object> mapRecord = new HashMap<String, Object>(2);
      mapRecord.put(field,   key);
      mapRecord.put("value", val);
      
      listResult.add(mapRecord);
    }
    
    if(debug) System.out.println(logprefix + "group(" + collection + "," + mapFilter + ",\"" + field + "\",\"" + groupFunction + "\") -> " + listResult.size() + " documents");
    return listResult;
  }
  
  @Override
  public
  Map<String, Object> read(String collection, String id)
      throws Exception
  {
    if(debug) System.out.println(logprefix + "read(" + collection + "," + id + ")...");
    
    List<Map<String, Object>> listColData = getCollectionData(dbname, collection, false);
    
    for(int i = 0; i < listColData.size(); i++) {
      Map<String, Object> mapItem = listColData.get(i);
      if(mapItem == null) continue;
      String sId = (String) mapItem.get("_id");
      if(sId != null && sId.equals(id)) {
        if(debug) System.out.println(logprefix + "read(" + collection + "," + id + ") -> {" + mapItem.size() + "}");
        return mapItem;
      }
    }
    
    if(debug) System.out.println(logprefix + "read(" + collection + "," + id + ") -> null");
    return null;
  }
  
  @Override
  public
  int count(String collection, Map<String, ?> mapFilter)
      throws Exception
  {
    if(debug) System.out.println(logprefix + "count(" + collection + "," + mapFilter + ")...");
    
    int result = 0;
    
    List<Map<String, Object>> listColData = getCollectionData(dbname, collection, false);
    
    Iterator<Map<String, Object>> iterator = listColData.iterator();
    while(iterator.hasNext()) {
      Map<String, Object> mapItem = iterator.next();
      if(mapItem == null) continue;
      if(match(mapItem, mapFilter)) {
        result++;
      }
    }
    
    if(debug) System.out.println(logprefix + "count(" + collection + "," + mapFilter + ") -> " + result);
    return result;
  }
  
  @Override
  public
  boolean createIndex(String collection, String field, int type)
      throws Exception
  {
    if(debug) System.out.println(logprefix + "createIndex(" + collection + "," + field + "," + type + ")...");
    boolean result = true;
    if(debug) System.out.println(logprefix + "createIndex(" + collection + "," + field + "," + type + ") -> " + result);
    return result;
  }
  
  @Override
  public 
  List<Map<String, Object>> listIndexes(String collection) 
      throws Exception
  {
    if(debug) System.out.println(logprefix + "listIndexes(" + collection + ")...");
    List<Map<String, Object>> listResult = new ArrayList<Map<String, Object>>();
    if(debug) System.out.println(logprefix + "listIndexes(" + collection + ") -> " + listResult);
    return listResult;
  }
  
  @Override
  public
  String writeFile(String filename, byte[] content, Map<String, ?> mapMetadata)
    throws Exception
  {
    return writeFile(filename, content, mapMetadata, null);
  }
  
  @Override
  public
  String writeFile(String filename, byte[] content, Map<String, ?> mapMetadata, Map<String, ?> mapAttributes)
    throws Exception
  {
    if(debug) {
      if(content == null) {
        System.out.println(logprefix + "writeFile(" + filename + ",null," + mapMetadata + "," + mapAttributes + ")...");
      }
      else {
        System.out.println(logprefix + "writeFile(" + filename + ",byte[" + content.length + "]," + mapMetadata + "," + mapAttributes + ")...");
      }
    }
    
    String id = generateId();
    
    String folderPath = System.getProperty("user.home") + File.separator + ".nosqlmock";
    
    File folder = new File(folderPath);
    if(!folder.exists()) folder.mkdirs();
    
    String filePath = folderPath + File.separator + filename;
    
    saveContent(content, filePath);
    
    List<Map<String, Object>> listColData = getCollectionData(dbname, "fs.files", true);
    
    Map<String, Object> mapFile = new HashMap<String, Object>();
    if(mapAttributes != null && !mapAttributes.isEmpty()) {
      mapFile.putAll(mapAttributes);
    }
    if(mapMetadata != null && !mapMetadata.isEmpty()) {
      mapFile.put("metadata", mapMetadata);
    }
    mapFile.put("_id",            id);
    mapFile.put(FILE_NAME,        filename);
    mapFile.put(FILE_LENGTH,      content != null ? content.length : 0);
    mapFile.put(FILE_DATE_UPLOAD, new Date());
    mapFile.put(FILE_MD5,         getDigestMD5(content));
    
    listColData.add(mapFile);
    
    if(debug) {
      if(content == null) {
        System.out.println(logprefix + "writeFile(" + filename + ",null," + mapMetadata + "," + mapAttributes + ") -> " + id);
      }
      else {
        System.out.println(logprefix + "writeFile(" + filename + ",byte[" + content.length + "]," + mapMetadata + "," + mapAttributes + ") -> " + id);
      }
    }
    return id;
  }
  
  @Override
  public
  List<Map<String, Object>> findFiles(String filename, Map<String, ?> mapFilter)
      throws Exception
  {
    if(debug) System.out.println(logprefix + "findFiles(" + filename + "," + mapFilter + ")...");
    
    List<Map<String, Object>> listResult = new ArrayList<Map<String, Object>>();
    
    List<Map<String, Object>> listColData = getCollectionData(dbname, "fs.files", false);
    
    Map<String, Object> mapQuery = new HashMap<String, Object>();
    if(mapFilter != null && !mapFilter.isEmpty()) {
      mapQuery.putAll(mapFilter);
    }
    if(filename != null && filename.length() > 0) {
      mapQuery.put(FILE_NAME, filename.replace('*', '%'));
    }
    
    Iterator<Map<String, Object>> iterator = listColData.iterator();
    while(iterator.hasNext()) {
      Map<String, Object> mapItem = iterator.next();
      if(mapItem == null) continue;
      if(match(mapItem, mapQuery)) {
        listResult.add(mapItem);
      }
    }
    
    if(debug) System.out.println(logprefix + "findFiles(" + filename + "," + mapFilter + ") -> " + listResult.size() + " files");
    return listResult;
  }
  
  @Override
  public
  Map<String, Object> readFile(String filename)
      throws Exception
  {
    if(debug) System.out.println(logprefix + "readFile(" + filename + ")...");
    
    String folderPath = System.getProperty("user.home") + File.separator + ".nosqlmock";
    
    File folder = new File(folderPath);
    if(!folder.exists()) folder.mkdirs();
    
    String filePath = folderPath + File.separator + filename;
    
    byte[] content = readContent(filePath);
    
    if(content == null || content.length == 0) {
      if(debug) System.out.println(logprefix + "readFile(" + filename + ") -> {0}");
      return new HashMap<String, Object>();
    }
    
    List<Map<String, Object>> listColData = getCollectionData(dbname, "fs.files", false);
    
    for(int i = 0; i < listColData.size(); i++) {
      Map<String, Object> mapItem = listColData.get(i);
      if(mapItem == null) continue;
      String sFileName = (String) mapItem.get(FILE_NAME);
      if(sFileName != null && sFileName.equals(filename)) {
        if(debug) System.out.println(logprefix + "readFile(" + filename + ") -> {" + mapItem.size() + "}");
        return mapItem;
      }
    }
    
    Map<String, Object> mapResult = new HashMap<String, Object>();
    mapResult.put(FILE_NAME,    filename);
    mapResult.put(FILE_CONTENT, content);
    mapResult.put(FILE_LENGTH,  content.length);
    mapResult.put(FILE_MD5,     getDigestMD5(content));
    
    if(debug) System.out.println(logprefix + "readFile(" + filename + ") -> {" + mapResult.size() + "}");
    return mapResult;
  }
  
  @Override
  public
  boolean removeFile(String filename)
      throws Exception
  {
    if(debug) System.out.println(logprefix + "removeFile(" + filename + ")...");
    
    String folderPath = System.getProperty("user.home") + File.separator + ".nosqlmock";
    
    File folder = new File(folderPath);
    if(!folder.exists()) folder.mkdirs();
    
    String filePath = folderPath + File.separator + filename;
    
    boolean result = false;
    
    File file = new File(filePath);
    if(file.exists()) {
      result = file.delete();
    }
    
    List<Map<String, Object>> listColData = getCollectionData(dbname, "fs.files", false);
    
    int iIndexOf = -1;
    for(int i = 0; i < listColData.size(); i++) {
      Map<String, Object> mapItem = listColData.get(i);
      if(mapItem == null) continue;
      String sFileName = (String) mapItem.get(FILE_NAME);
      if(sFileName != null && sFileName.equals(filename)) {
        iIndexOf = i;
        break;
      }
    }
    
    if(iIndexOf >= 0) {
      listColData.remove(iIndexOf);
    }
    
    if(debug) System.out.println(logprefix + "removeFile(" + filename + ") -> " + result);
    return result;
  }
  
  @Override
  public
  boolean renameFile(String filename, String newFilename)
      throws Exception
  {
    if(debug) System.out.println(logprefix + "renameFile(" + filename + "," + newFilename + ")...");
    
    String folderPath = System.getProperty("user.home") + File.separator + ".nosqlmock";
    
    File folder = new File(folderPath);
    if(!folder.exists()) folder.mkdirs();
    
    String filePath = folderPath + File.separator + filename;
    String fileDest = folderPath + File.separator + newFilename;
    
    boolean result = false;
    
    File file = new File(filePath);
    if(file.exists()) {
      result = file.renameTo(new File(fileDest));
    }
    
    List<Map<String, Object>> listColData = getCollectionData(dbname, "fs.files", false);
    
    for(int i = 0; i < listColData.size(); i++) {
      Map<String, Object> mapItem = listColData.get(i);
      if(mapItem == null) continue;
      String sFileName = (String) mapItem.get(FILE_NAME);
      if(sFileName != null && sFileName.equals(filename)) {
        mapItem.put(FILE_NAME, newFilename);
      }
    }
    
    if(debug) System.out.println(logprefix + "renameFile(" + filename + "," + newFilename + ") -> " + result);
    return result;
  }
  
  protected static
  List<Map<String, Object>> getCollectionData(String dbname, String collection, boolean createIfNotExists)
  {
    List<Map<String, Object>> listColData = null;
    
    if(data == null) data = new HashMap<String, Map<String,List<Map<String, Object>>>>();
    
    Map<String,List<Map<String, Object>>> mapColData = data.get(dbname);
    if(mapColData == null) {
      if(!createIfNotExists) {
        return new ArrayList<Map<String, Object>>();
      }
      mapColData = new HashMap<String,List<Map<String, Object>>>();
      data.put(dbname, mapColData);
      listColData = new ArrayList<Map<String, Object>>();
      mapColData.put(collection, listColData);
    }
    else {
      listColData = mapColData.get(collection);
      if(listColData == null) {
        if(!createIfNotExists) {
          return new ArrayList<Map<String, Object>>();
        }
        listColData = new ArrayList<Map<String, Object>>();
        mapColData.put(collection, listColData);
      }
    }
    
    return listColData;
  }
  
  protected static
  String generateId()
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
  
  protected static
  String getOrderField(String sOrderClause)
  {
    int iSep = sOrderClause.indexOf(' ');
    if(iSep > 0) return sOrderClause.substring(0, iSep);
    return sOrderClause;
  }
  
  protected static
  String getOrderType(String sOrderClause)
  {
    int iSep = sOrderClause.indexOf(' ');
    if(iSep > 0) {
      char c0 = sOrderClause.substring(iSep+1).charAt(0);
      if(c0 == 'd' || c0 == 'D' || c0 == '-') return ":desc";
      return ":asc";
    }
    return "";
  }
  
  protected static
  boolean match(Map mapItem, Map mapFilter)
  {
    if(mapFilter == null || mapFilter.isEmpty()) {
      return true;
    }
    if(mapItem == null) {
      return false;
    }
    
    Iterator iterator = mapFilter.entrySet().iterator();
    while(iterator.hasNext()) {
      Map.Entry entry = (Map.Entry) iterator.next();
      
      Object key = entry.getKey();
      
      String sKey = key.toString();
      if(sKey.equals(FILTER_EXCLUDE) || sKey.equals(FILTER_FIELDS)) continue;
      
      Object val = entry.getValue();
      String sVal = WUtil.toString(val, "null");
      
      boolean boStartsWithPerc = false;
      boolean boEndsWithPerc   = false;
      boStartsWithPerc = sKey.startsWith("%");
      if(boStartsWithPerc) sKey = sKey.substring(1);
      boEndsWithPerc = sKey.endsWith("%");
      if(boEndsWithPerc) sKey = sKey.substring(0, sKey.length()-1);
      
      boolean boGTE  = sKey.startsWith(">=");
      boolean boLTE  = sKey.startsWith("<=");
      boolean boNE   = sKey.startsWith("<>");
      if(!boNE) boNE = sKey.startsWith("!=");
      if(boGTE || boLTE || boNE) {
        sKey = sKey.substring(2);
      }
      else {
        boGTE  = sKey.endsWith(">=");
        boLTE  = sKey.endsWith("<=");
        boNE   = sKey.endsWith("<>");
        if(!boNE) boNE = sKey.endsWith("!=");
        if(boGTE || boLTE || boNE) {
          sKey = sKey.substring(0, sKey.length()-2);
        }
      }
      
      boolean boGT  = sKey.startsWith(">");
      boolean boLT  = sKey.startsWith("<");
      if(boGT || boLT) {
        sKey = sKey.substring(1);
      }
      else {
        boGT = sKey.endsWith(">");
        boLT = sKey.endsWith("<");
        if(boGT || boLT) {
          sKey = sKey.substring(0, sKey.length()-1);
        }
      }
      
      Object ivl = get(mapItem, sKey);
      int cmp = 0;
      if(ivl == null && val == null) {
        cmp = 0; // equals
      }
      else if(ivl == null && val != null) {
        cmp = 1;
      }
      else if(ivl != null && val == null) {
        cmp = -1;
      }
      else if(ivl instanceof Comparable && val instanceof Comparable) {
        cmp = ((Comparable) ivl).compareTo(val);
      }
      
      if(sVal != null && !(boGTE || boLTE || boNE || boGT || boLT)) {
        boGTE  = sVal.startsWith(">=");
        boLTE  = sVal.startsWith("<=");
        boNE   = sVal.startsWith("<>");
        if(!boNE) boNE = sVal.startsWith("!=");
        if(boGTE || boLTE || boNE) sVal = sVal.substring(2);
        
        boGT   = sVal.startsWith(">");
        boLT   = sVal.startsWith("<");
        if(boGT || boLT) sVal = sVal.substring(1);
      }
      
      if(sVal.startsWith("%")) {
        sVal = sVal.substring(1);
        boStartsWithPerc = true;
      }
      if(sVal.endsWith("%")) {
        sVal = sVal.substring(0, sVal.length()-1);
        boEndsWithPerc = true;
      }
      
      if(val instanceof Collection) {
        if(boNE || boGT || boLT) {
          if(((Collection) val).contains(ivl)) return false;
        }
        else {
          if(!((Collection) val).contains(ivl)) return false;
        }
        continue;
      }
      if(val != null && val.getClass().isArray()) {
        if(boNE || boGT || boLT) {
          int length = Array.getLength(val);
          for(int i = 0; i < length; i++) {
            Object oi = Array.get(val, i);
            if(oi != null && oi.equals(ivl)) return false;
          }
        }
        else {
          int length = Array.getLength(val);
          boolean boAtLeastOne = false;
          for(int i = 0; i < length; i++) {
            Object oi = Array.get(val, i);
            if(oi != null && oi.equals(ivl)) boAtLeastOne = true;
          }
          if(!boAtLeastOne) return false;
        }
        continue;
      }
      
      if(sVal.equals("null")) {
        if(boNE) {
          if(ivl == null) return false;
        }
        else {
          if(ivl != null) return false;
        }
        continue;
      }
      
      if(boNE) {
        if(val == null && ivl == null)     return false;
        if(val != null && val.equals(ivl)) return false;
      }
      else if(boGT) {
        if(cmp < 1) return false;
      }
      else if(boLT) {
        if(cmp > -1) return false;
      }
      else if(boGTE) {
        if(cmp == -1) return false;
      }
      else if(boLTE) {
        if(cmp == 1) return false;
      }
      else {
        if(boStartsWithPerc || boEndsWithPerc) {
          String sIvl = WUtil.toString(ivl, "null");
          if(boStartsWithPerc && boEndsWithPerc) {
            if(sIvl.indexOf(sIvl) < 0) return false;
          }
          else if(boStartsWithPerc) {
            if(!sIvl.endsWith(sVal)) return false;
          }
          else if(boEndsWithPerc) {
            if(!sIvl.startsWith(sVal)) return false;
          }
        }
        else {
          if(cmp != 0) return false;
        }
      }
    }
    return true;
  }
  
  protected static
  boolean match(Map mapItem, String field, String text)
  {
    if(field == null || field.length() == 0) {
      return true;
    }
    if(mapItem == null) {
      return false;
    }
    
    String sText = WUtil.toString(mapItem.get(field), null);
    if(sText == null && text == null) return true;
    if(sText == null || text == null) return false;
    
    return sText.toLowerCase().indexOf(text.toLowerCase()) >= 0;
  }
  
  protected static 
  Object get(Map src, String keysrc) 
  {
    Object  valsrc = null;
    int iSep = keysrc.indexOf('.');
    if(iSep > 0) {
      String key1 = keysrc.substring(0, iSep);
      Object val1 = get(src, key1);
      if(val1 instanceof Map) {
        Map map1 = (Map) val1;
        String key2 = keysrc.substring(iSep + 1);
        valsrc = get(map1, key2);
      }
    }
    else {
      valsrc = src.get(keysrc);
    }
    return valsrc;
  }
  
  protected static
  Map<String, Object> mapObject(Map map)
  {
    if(map == null) {
      return new HashMap<String, Object>();
    }
    return (Map<String, Object>) map;
  }
  
  protected static
  List reverse(List list)
  {
    if(list == null) return null;
    List listResult = new ArrayList(list.size());
    for(int i=list.size()-1; i >= 0; i--) {
      listResult.add(list.get(i));
    }
    return listResult;
  }
  
  protected static
  Object findVal(Map mapFilter, String sStarstWith, String sEndsWith)
  {
    Iterator iterator = mapFilter.entrySet().iterator();
    while(iterator.hasNext()) {
      Map.Entry entry = (Map.Entry) iterator.next();
      Object oKey = entry.getKey();
      String sKey = oKey.toString();
      if(!sKey.startsWith(sStarstWith)) continue;
      if(!sKey.endsWith(sEndsWith))     continue;
      return entry.getValue();
    }
    return null;
  }
  
  protected static
  String getDigestMD5(byte[] content)
      throws Exception
  {
    if(content == null) return "";
    try {
      MessageDigest md = MessageDigest.getInstance("MD5");
      md.update(content);
      return String.valueOf(Base64Coder.encode(md.digest()));
    }
    catch(Exception ex) {
    }
    return "-";
  }
  
  protected static
  byte[] readContent(String filePath)
      throws Exception
  {
    int iFileSep = filePath.indexOf('/');
    if(iFileSep < 0) iFileSep = filePath.indexOf('\\');
    InputStream is = null;
    if(iFileSep < 0) {
      URL url = Thread.currentThread().getContextClassLoader().getResource(filePath);
      is = url.openStream();
    }
    else {
      is = new FileInputStream(filePath);
    }
    try {
      int n;
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      byte[] buff = new byte[1024];
      while((n = is.read(buff)) > 0) baos.write(buff, 0, n);
      return baos.toByteArray();
    }
    finally {
      if(is != null) try{ is.close(); } catch(Exception ex) {}
    }
  }
  
  protected static
  void saveContent(byte[] content, String filePath)
      throws Exception
  {
    if(content == null) return;
    if(content == null || content.length == 0) return;
    FileOutputStream fos = null;
    try {
      fos = new FileOutputStream(filePath);
      fos.write(content);
    }
    finally {
      if(fos != null) try{ fos.close(); } catch(Exception ex) {}
    }
  }
  
  protected static 
  int[] loadFile(String filePath)
    throws Exception
  {
    // [0] = count database, [1] = count collections, [2] = count records
    int[] result = {0, 0, 0};
    
    if(filePath == null || filePath.length() == 0) {
      filePath = System.getProperty("user.home") + File.separator + "nosqlmock.json";
    }
    filePath = filePath.replace("$HOME", System.getProperty("user.home"));
    
    String firstDatabase = null;
    
    // Load content
    Map<String, Object> mapContent = null;
    File file = new File(filePath);
    if(file.exists()) {
      byte[] content = readContent(filePath);
      if(content != null && content.length > 0) {
        String sContent = new String(content);
        mapContent = JSON.parseObj(sContent);
      }
    }
    
    // Check content
    if(mapContent == null || mapContent.isEmpty()) {
      return result;
    }
    
    // Clear data
    if(data == null) data = new HashMap<String, Map<String,List<Map<String, Object>>>>();
    data.clear();
    
    // Import data
    Iterator<Map.Entry<String, Object>> iteratorDB = mapContent.entrySet().iterator();
    while(iteratorDB.hasNext()) {
      Map.Entry<String, Object> entryDB = iteratorDB.next();
      
      String sDatabase   = entryDB.getKey();
      Object collections = entryDB.getValue();
      
      // Check database data
      if(sDatabase.length() == 0) continue;
      if(!(collections instanceof Map)) continue;
      Map<String, Object> mapCollections = WUtil.toMapObject(collections);
      if(mapCollections == null) continue;
      
      Iterator<Map.Entry<String, Object>> iteratorCol = mapCollections.entrySet().iterator();
      while(iteratorCol.hasNext()) {
        Map.Entry<String, Object> entryCol = iteratorCol.next();
        
        String sCollection    = entryCol.getKey();
        Object collectionData = entryCol.getValue();
        
        // Check collection data
        if(sCollection.length() == 0) continue;
        if(!(collectionData instanceof List)) continue;
        List<Map<String, Object>> listCollectionData = WUtil.toListOfMapObject(collectionData);
        if(listCollectionData == null) continue;
        
        Map<String,List<Map<String, Object>>> mapDBDataCollection = data.get(sDatabase);
        if(mapDBDataCollection == null) {
          mapDBDataCollection = new HashMap<String, List<Map<String, Object>>>();
          data.put(sDatabase, mapDBDataCollection);
          result[0]++;
          firstDatabase = sDatabase;
        }
        
        mapDBDataCollection.put(sCollection, listCollectionData);
        result[1]++;
        
        result[2] += listCollectionData.size();
      }
    }
    
    if(result[0] == 1 && firstDatabase != null && firstDatabase.length() > 0) {
      defaultDatabase = firstDatabase;
    }
    
    return result;
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
