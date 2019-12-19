package org.dew.test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.dew.nosql.INoSQLDB;
import org.dew.nosql.NoSQLMock;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

public class TestNOSQL extends TestCase {
  
  public TestNOSQL(String testName) {
    super(testName);
  }
  
  public static Test suite() {
    return new TestSuite(TestNOSQL.class);
  }
  
  public void testApp() {
    try {
      INoSQLDB nosqlDB = new NoSQLMock();
      
      System.out.println("insert...");
      nosqlDB.insert("users", buildMap("name", "MARIO",    "family", "ROSSI",   "age", 44, "city", "ROME"));
      nosqlDB.insert("users", buildMap("name", "GIUSEPPE", "family", "VERDI",   "age", 40, "city", "ROME"));
      nosqlDB.insert("users", buildMap("name", "ANTONIO",  "family", "BIANCHI", "age", 28, "city", "ROME"));
      nosqlDB.insert("users", buildMap("name", "CARLO",    "family", "GIALLI",  "age", 36, "city", "LONDON"));
      nosqlDB.insert("users", buildMap("name", "ROBERTO",  "family", "NERI",    "age", 32, "city", "LONDON"));
      
      System.out.println("nosqlDB.getCollections...");
      List<String> collections = nosqlDB.getCollections();
      printList("collections:", collections);
      
      System.out.println("nosqlDB.find...");
      List<Map<String,Object>> listResult = nosqlDB.find("users", buildMap("name", "M%"), "*");
      printList("find result:", listResult);
      
      System.out.println("nosqlDB.update...");
      nosqlDB.update("users", buildMap("age", 45), buildMap("name", "MARIO"));
      
      System.out.println("nosqlDB.find...");
      listResult = nosqlDB.find("users", buildMap("name", "MARIO"), "*");
      printList("find result:", listResult);
      
      System.out.println("nosqlDB.delete...");
      nosqlDB.delete("users", buildMap("name", "MARIO"));
      
      System.out.println("nosqlDB.find...");
      listResult = nosqlDB.find("users", buildMap("name", "MARIO"), "*");
      printList("find result:", listResult);
      
      System.out.println("nosqlDB.find...");
      listResult = nosqlDB.find("users", buildMap("age<", 30), "*");
      printList("find result:", listResult);
      
      System.out.println("nosqlDB.group...");
      listResult = nosqlDB.group("users", new HashMap<String,Object>(), "city", "count");
      printList("group result:", listResult);
    }
    catch(Exception ex) {
      ex.printStackTrace();
    }
  }
  
  protected static
  Map<String,Object> buildMap(String k0, Object v0)
  {
    Map<String,Object> mapResult = new HashMap<String, Object>();
    if(k0 != null && k0.length() > 0) mapResult.put(k0, v0);
    return mapResult;
  }
  
  protected static
  Map<String,Object> buildMap(String k0, Object v0, String k1, Object v1)
  {
    Map<String,Object> mapResult = new HashMap<String, Object>();
    if(k0 != null && k0.length() > 0) mapResult.put(k0, v0);
    if(k1 != null && k1.length() > 0) mapResult.put(k1, v1);
    return mapResult;
  }
  
  protected static
  Map<String,Object> buildMap(String k0, Object v0, String k1, Object v1, String k2, Object v2)
  {
    Map<String,Object> mapResult = new HashMap<String, Object>();
    if(k0 != null && k0.length() > 0) mapResult.put(k0, v0);
    if(k1 != null && k1.length() > 0) mapResult.put(k1, v1);
    if(k2 != null && k2.length() > 0) mapResult.put(k2, v2);
    return mapResult;
  }
  
  protected static
  Map<String,Object> buildMap(String k0, Object v0, String k1, Object v1, String k2, Object v2, String k3, Object v3)
  {
    Map<String,Object> mapResult = new HashMap<String, Object>();
    if(k0 != null && k0.length() > 0) mapResult.put(k0, v0);
    if(k1 != null && k1.length() > 0) mapResult.put(k1, v1);
    if(k2 != null && k2.length() > 0) mapResult.put(k2, v2);
    if(k3 != null && k3.length() > 0) mapResult.put(k3, v3);
    return mapResult;
  }
  
  protected static
  void printList(String title, List<?> list)
  {
    if(title != null) {
      System.out.println(title);
    }
    if(list == null) {
      System.out.println("    list is null");
    }
    else if(list.size() == 0) {
      System.out.println("    list is empty");
    }
    else {
      for(int i = 0; i < list.size(); i++) {
        System.out.println("    [" + i + "] " + list.get(i));
      }
    }
  }
}
