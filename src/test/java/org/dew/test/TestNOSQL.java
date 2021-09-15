package org.dew.test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.http.HttpHost;

import org.dew.nosql.*;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.core.MainResponse;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;

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
      nosqlDB.setDebug(true);
      
      nosqlDB.insert("users", map("name", "MARIO",    "family", "ROSSI",   "age", 44, "city", "ROME"));
      nosqlDB.insert("users", map("name", "GIUSEPPE", "family", "VERDI",   "age", 40, "city", "ROME"));
      nosqlDB.insert("users", map("name", "ANTONIO",  "family", "BIANCHI", "age", 28, "city", "ROME"));
      nosqlDB.insert("users", map("name", "CARLO",    "family", "GIALLI",  "age", 36, "city", "LONDON"));
      nosqlDB.insert("users", map("name", "ROBERTO",  "family", "NERI",    "age", 32, "city", "LONDON"));
      
      List<String> collections = nosqlDB.getCollections();
      print("collections:", collections);
      
      List<Map<String,Object>> listResult = nosqlDB.find("users", map("name", "M%"), "*");
      print("find result:", listResult);
      
      nosqlDB.update("users", map("age", 45), map("name", "MARIO"));
      
      listResult = nosqlDB.find("users", map("name", "MARIO"), "*");
      print("find result:", listResult);
      
      nosqlDB.delete("users", map("name", "MARIO"));
      
      listResult = nosqlDB.find("users", map("name", "MARIO"), "*");
      print("find result:", listResult);
      
      listResult = nosqlDB.find("users", map("age<", 30), "*");
      print("find result:", listResult);
      
      listResult = nosqlDB.group("users", new HashMap<String,Object>(), "city", "count");
      print("group result:", listResult);
    }
    catch(Exception ex) {
      ex.printStackTrace();
    }
  }
  
  public 
  void testES() 
    throws Exception 
  {
    String sOperation = System.getProperty("dew.test.op", "");
    if(sOperation == null || !sOperation.equalsIgnoreCase("testES")) return;
    
    HttpHost httpHost = new HttpHost("localhost", 9200, HttpHost.DEFAULT_SCHEME_NAME);
    
    try(RestHighLevelClient client = new RestHighLevelClient(RestClient.builder(httpHost))) {
      
      MainResponse response = client.info(RequestOptions.DEFAULT);
      
      System.out.println(response.getClusterName());
      System.out.println(response.getNodeName());
      System.out.println(response.getVersion().getNumber());
      
      SearchRequest searchRequest = new SearchRequest("test");
      
      SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
      
      SearchHits searchHits = searchResponse.getHits();
      
      long totalHits = searchHits.getTotalHits().value;
      
      for(int i = 0; i < totalHits; i++) {
        SearchHit searchHit = searchHits.getAt(i);
        System.out.println(searchHit.getId() + " " + searchHit.getSourceAsMap());
      }
    }
    catch(Exception ex) {
      ex.printStackTrace();
    }
  }
  
  protected static
  Map<String,Object> map(String k0, Object v0)
  {
    Map<String,Object> mapResult = new HashMap<String, Object>();
    if(k0 != null && k0.length() > 0) mapResult.put(k0, v0);
    return mapResult;
  }
  
  protected static
  Map<String,Object> map(String k0, Object v0, String k1, Object v1)
  {
    Map<String,Object> mapResult = new HashMap<String, Object>();
    if(k0 != null && k0.length() > 0) mapResult.put(k0, v0);
    if(k1 != null && k1.length() > 0) mapResult.put(k1, v1);
    return mapResult;
  }
  
  protected static
  Map<String,Object> map(String k0, Object v0, String k1, Object v1, String k2, Object v2)
  {
    Map<String,Object> mapResult = new HashMap<String, Object>();
    if(k0 != null && k0.length() > 0) mapResult.put(k0, v0);
    if(k1 != null && k1.length() > 0) mapResult.put(k1, v1);
    if(k2 != null && k2.length() > 0) mapResult.put(k2, v2);
    return mapResult;
  }
  
  protected static
  Map<String,Object> map(String k0, Object v0, String k1, Object v1, String k2, Object v2, String k3, Object v3)
  {
    Map<String,Object> mapResult = new HashMap<String, Object>();
    if(k0 != null && k0.length() > 0) mapResult.put(k0, v0);
    if(k1 != null && k1.length() > 0) mapResult.put(k1, v1);
    if(k2 != null && k2.length() > 0) mapResult.put(k2, v2);
    if(k3 != null && k3.length() > 0) mapResult.put(k3, v3);
    return mapResult;
  }
  
  protected static
  Map<String,Object> map(String k0, Object v0, String k1, Object v1, String k2, Object v2, String k3, Object v3, String k4, Object v4)
  {
    Map<String,Object> mapResult = new HashMap<String, Object>();
    if(k0 != null && k0.length() > 0) mapResult.put(k0, v0);
    if(k1 != null && k1.length() > 0) mapResult.put(k1, v1);
    if(k2 != null && k2.length() > 0) mapResult.put(k2, v2);
    if(k3 != null && k3.length() > 0) mapResult.put(k3, v3);
    if(k4 != null && k4.length() > 0) mapResult.put(k4, v4);
    return mapResult;
  }
  
  protected static
  void print(String title, List<?> list)
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
