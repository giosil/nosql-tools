package org.dew.nosql;

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public 
class NoSQLBlank implements INoSQLDB 
{
  protected static String logprefix = NoSQLBlank.class.getSimpleName() + ".";
  
  protected boolean debug = false;
  protected PrintStream log = System.out;
  protected int currentId = 1;
  
  public NoSQLBlank()
  {
  }
  
  public NoSQLBlank(boolean debug)
  {
    this.debug = debug;
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
    mapResult.put("name",    "Blank");
    mapResult.put("version", "1.0.0");
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
    mapResult.put("name",    "Blank");
    mapResult.put("version", "1.0.0");
    if(debug) log.println(logprefix + "getInfo() -> " + mapResult);
    return mapResult;
  }

  @Override
  public List<String> getCollections() throws Exception {
    if(debug) log.println(logprefix + "getCollections()...");
    List<String> listResult = new ArrayList<String>();
    if(debug) log.println(logprefix + "getCollections() -> " + listResult);
    return listResult;
  }

  @Override
  public boolean drop(String collection) throws Exception {
    if(debug) log.println(logprefix + "drop(" + collection + ")...");
    boolean result = false;
    if(debug) log.println(logprefix + "drop(" + collection + ") -> " + result);
    return result;
  }

  @Override
  public String insert(String collection, Map<String, ?> mapData) throws Exception {
    if(debug) log.println(logprefix + "insert(" + collection + "," + mapData + ")...");
    String result = String.valueOf(++currentId);
    if(debug) log.println(logprefix + "insert(" + collection + "," + mapData + ") -> " + result);
    return result;
  }

  @Override
  public String insert(String collection, Map<String, ?> mapData, boolean refresh) throws Exception {
    if(debug) log.println(logprefix + "insert(" + collection + "," + mapData + "," + refresh + ")...");
    String result = String.valueOf(++currentId);
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
    log.println(logprefix + "bulkIns(" + collection + ", " + listData.size() + " documents) -> " + result);
    return result;
  }

  @Override
  public boolean replace(String collection, Map<String, ?> mapData, String id) throws Exception {
    if(debug) log.println(logprefix + "replace(" + collection + "," + mapData + ")...");
    boolean result = false;
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
    if(debug) log.println(logprefix + "update(" + collection + "," + mapData + "," + mapFilter + ") -> " + result);
    return result;
  }

  @Override
  public String upsert(String collection, Map<String, ?> mapData, Map<String, ?> mapFilter) throws Exception {
    if(debug) log.println(logprefix + "upsert(" + collection + "," + mapData + "," + mapFilter + ")...");
    String result = String.valueOf(++currentId);
    if(debug) log.println(logprefix + "upsert(" + collection + "," + mapData + "," + mapFilter + ") -> " + result);
    return result;
  }

  @Override
  public int unset(String collection, String fields, String id) throws Exception {
    if(debug) log.println(logprefix + "unset(" + collection + "," + fields + "," + id + ")...");
    int result = 0;
    if(debug) log.println(logprefix + "unset(" + collection + "," + fields + "," + id + ") -> " + result);
    return result;
  }

  @Override
  public int inc(String collection, String id, String field, Number value) throws Exception {
    if(debug) log.println(logprefix + "inc(" + collection + "," + id + "," + field + "," + value + ")...");
    int result = 0;
    if(debug) log.println(logprefix + "inc(" + collection + "," + id + "," + field + "," + value + ") -> " + result);
    return result;
  }

  @Override
  public int inc(String collection, String id, String field1, Number value1, String field2, Number value2) throws Exception {
    if(debug) log.println(logprefix + "inc(" + collection + "," + id + "," + field1 + "," + value1 + "," + field2 + "," + value2 + ")...");
    int result = 0;
    if(debug) log.println(logprefix + "inc(" + collection + "," + id + "," + field1 + "," + value1 + "," + field2 + "," + value2 + ") -> " + result);
    return result;
  }

  @Override
  public int inc(String collection, Map<String, ?> mapFilter, String field, Number value) throws Exception {
    if(debug) log.println(logprefix + "inc(" + collection + "," + mapFilter + "," + field + "," + value + ")...");
    int result = 0;
    if(debug) log.println(logprefix + "inc(" + collection + "," + mapFilter + "," + field + "," + value + ") -> " + result);
    return result;
  }

  @Override
  public int inc(String collection, Map<String, ?> mapFilter, String field1, Number value1, String field2, Number value2) throws Exception {
    if(debug) log.println(logprefix + "inc(" + collection + "," + mapFilter + "," + field1 + "," + value1 + "," + field2 + "," + value2 + ")...");
    int result = 0;
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
    if(debug) log.println(logprefix + "delete(" + collection + "," + mapFilter + ") -> " + result);
    return result;
  }

  @Override
  public List<Map<String, Object>> find(String collection, Map<String, ?> mapFilter, String fields) throws Exception {
    if(debug) log.println(logprefix + "find(" + collection + "," + mapFilter + ",\"" + fields + "\")...");
    List<Map<String, Object>> listResult = new ArrayList<Map<String, Object>>();
    if(debug) log.println(logprefix + "find(" + collection + "," + mapFilter + ",\"" + fields + "\") -> " + listResult.size() + " documents");
    return listResult;
  }

  @Override
  public List<Map<String, Object>> find(String collection, Map<String, ?> mapFilter, String fields, String orderBy, int limit) throws Exception {
    if(debug) log.println(logprefix + "find(" + collection + "," + mapFilter + ",\"" + fields + "\",\"" + orderBy + "\"," + limit + ")...");
    List<Map<String, Object>> listResult = new ArrayList<Map<String, Object>>();
    if(debug) log.println(logprefix + "find(" + collection + "," + mapFilter + ",\"" + fields + "\",\"" + orderBy + "\"," + limit + ") -> " + listResult.size() + " documents");
    return listResult;
  }

  @Override
  public List<Map<String, Object>> find(String collection, Map<String, ?> mapFilter, String fields, String orderBy, int limit, int skip) throws Exception {
    if(debug) log.println(logprefix + "find(" + collection + "," + mapFilter + ",\"" + fields + "\",\"" + orderBy + "\"," + limit + "," + skip + ")...");
    List<Map<String, Object>> listResult = new ArrayList<Map<String, Object>>();
    if(debug) log.println(logprefix + "find(" + collection + "," + mapFilter + ",\"" + fields + "\",\"" + orderBy + "\"," + limit + "," + skip + ") -> " + listResult.size() + " documents");
    return listResult;
  }

  @Override
  public List<Map<String, Object>> search(String collection, String field, String text) throws Exception {
    if(debug) log.println(logprefix + "search(" + collection + "," + field + "," + text + ")...");
    List<Map<String, Object>> listResult = new ArrayList<Map<String, Object>>();
    if(debug) log.println(logprefix + "search(" + collection + "," + field + "," + text + ") -> " + listResult.size() + " documents");
    return listResult;
  }

  @Override
  public List<Map<String, Object>> group(String collection, Map<String, ?> mapFilter, String field, String groupFunction) throws Exception {
    if(debug) log.println(logprefix + "group(" + collection + "," + mapFilter + ",\"" + field + "\",\"" + groupFunction + "\")...");
    List<Map<String, Object>> listResult = new ArrayList<Map<String, Object>>();
    if(debug) log.println(logprefix + "group(" + collection + "," + mapFilter + ",\"" + field + "\",\"" + groupFunction + "\") -> " + listResult.size() + " documents");
    return listResult;
  }

  @Override
  public Map<String, Object> read(String collection, String id) throws Exception {
    if(debug) log.println(logprefix + "read(" + collection + "," + id + ")...");
    Map<String, Object> mapResult = new HashMap<String, Object>();
    if(debug) log.println(logprefix + "read(" + collection + "," + id + ") -> " + mapResult);
    return mapResult;
  }

  @Override
  public int count(String collection, Map<String, ?> mapFilter) throws Exception {
    if(debug) log.println(logprefix + "count(" + collection + "," + mapFilter + ")...");
    int result = 0;
    if(debug) log.println(logprefix + "count(" + collection + "," + mapFilter + ") -> " + result);
    return result;
  }

  @Override
  public
  boolean createIndex(String collection, String field, int type) throws Exception {
    if(debug) log.println(logprefix + "createIndex(" + collection + "," + field + "," + type + ")...");
    boolean result = true;
    if(debug) log.println(logprefix + "createIndex(" + collection + "," + field + "," + type + ") -> " + result);
    return result;
  }
  
  @Override
  public List<Map<String, Object>> listIndexes(String collection) throws Exception {
    if(debug) log.println(logprefix + "listIndexes(" + collection + ")...");
    List<Map<String, Object>> listResult = new ArrayList<Map<String, Object>>();
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
    if(debug) log.println(logprefix + "findFiles(" + filename + "," + mapFilter + ") -> " + listResult.size() + " files");
    return listResult;
  }

  @Override
  public Map<String, Object> readFile(String filename) throws Exception {
    if(debug) log.println(logprefix + "readFile(" + filename + ")...");
    Map<String, Object> mapResult = new HashMap<String, Object>();
    if(debug) log.println(logprefix + "readFile(" + filename + ") -> {" + mapResult.size() + "}");
    return mapResult;
  }

  @Override
  public boolean removeFile(String filename) throws Exception {
    if(debug) log.println(logprefix + "removeFile(" + filename + ")...");
    boolean result = false;
    if(debug) log.println(logprefix + "removeFile(" + filename + ") -> " + result);
    return result;
  }

  @Override
  public boolean renameFile(String filename, String newFilename) throws Exception {
    if(debug) log.println(logprefix + "renameFile(" + filename + "," + newFilename + ")...");
    boolean result = false;
    if(debug) log.println(logprefix + "renameFile(" + filename + "," + newFilename + ") -> " + result);
    return result;
  }
}
