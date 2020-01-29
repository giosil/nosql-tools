package org.dew.nosql;

import java.io.InputStream;

import java.net.URL;

import java.util.Properties;

import org.dew.nosql.util.WUtil;

public 
class NoSQLDataSource 
{
  public static final boolean DEBUG = false;
  
  public static Properties config = new Properties();
  
  static {
    InputStream is = null;
    try {
      URL urlCfg = Thread.currentThread().getContextClassLoader().getResource("nosql.cfg");
      if(urlCfg != null) {
        is = urlCfg.openStream();
        config.load(is);
      }
      else {
        System.out.println("GRAVE: nosql.cfg not found in classpath.");
      }
    }
    catch(Exception ex) {
      ex.printStackTrace();
    }
    finally {
      try { is.close(); } catch (Exception oEx) {}
    }
  }
  
  public static
  String getProperty(String sKey)
  {
    return config.getProperty(sKey);
  }
  
  public static
  String getProperty(String sKey, String sDefault)
  {
    return config.getProperty(sKey, sDefault);
  }
  
  public static
  String getProperty(String sKey, boolean boMandatory)
    throws Exception
  {
    String sResult = config.getProperty(sKey);
    if(boMandatory) {
      if(sResult == null || sResult.length() == 0) {
        throw new Exception("Entry \"" + sKey + "\" of configuration is blank.");
      }
    }
    return sResult;
  }
  
  public static
  int getIntProperty(String sKey, int iDefault)
  {
    String sValue = config.getProperty(sKey);
    return WUtil.toInt(sValue, iDefault);
  }
  
  public static
  boolean getBooleanProperty(String sKey, boolean bDefault)
  {
    String sValue = config.getProperty(sKey);
    return WUtil.toBoolean(sValue, bDefault);
  }
  
  public static
  String[] parseUrl(String url)
  {
    if(url == null || url.length() == 0) {
      return null;
    }
    int iSepPro = url.indexOf("//");
    if(iSepPro < 0) return null;
    
    String sInfo = "";
    int iSepOpt = url.indexOf('?');
    if(iSepOpt > iSepPro) {
      sInfo = url.substring(iSepPro + 2, iSepOpt).trim();
    }
    else {
      sInfo = url.substring(iSepPro + 2).trim();
    }
    if(sInfo == null || sInfo.length() == 0) {
      return null;
    }
    
    String sHost   = "";
    int    iPort   = 80;
    String sDB     = "";
    String sUser   = "";
    String sPass   = "";
    
    int iSepDB   = sInfo.indexOf('/');
    if(iSepDB <= 0) {
      iSepDB = sInfo.length();
    }
    else {
      sDB = sInfo.substring(iSepDB + 1);
      int iSub = sDB.indexOf('/');
      if(iSub > 0) {
        sDB = sDB.substring(0, iSub);
      }
    }
    
    int iSepCred = sInfo.indexOf('@');
    if(iSepCred > 0) {
      int iSepUsrPas = sInfo.indexOf(':');
      if(iSepUsrPas > 0 && iSepUsrPas < iSepCred) {
        sUser = sInfo.substring(0, iSepUsrPas);
        sPass = sInfo.substring(iSepUsrPas + 1, iSepCred);
      }
      else {
        sUser = sInfo.substring(0, iSepCred);
        sPass = "";
      }
      int iSepHostPort = sInfo.lastIndexOf(':');
      if(iSepHostPort > iSepCred) {
        sHost = sInfo.substring(iSepCred+1, iSepHostPort);
        try{ iPort = Integer.parseInt(sInfo.substring(iSepHostPort+1, iSepDB)); } catch(Throwable ex) {}
        if(iPort < 20) iPort = 27017;
      }
      else {
        sHost = sInfo.substring(iSepCred+1, iSepDB);
      }
    }
    else {
      int iSepHostPort = sInfo.lastIndexOf(':');
      if(iSepHostPort > 0) {
        sHost = sInfo.substring(0, iSepHostPort);
        try{ iPort = Integer.parseInt(sInfo.substring(iSepHostPort+1, iSepDB)); } catch(Throwable ex) {}
        if(iPort < 20) iPort = 27017;
      }
      else {
        sHost = sInfo.substring(0, iSepDB);
      }
    }
    
    if(sHost == null || sHost.length() == 0) {
      return null;
    }
    
    //                   0      1                      2    3      4
    return new String[] {sHost, String.valueOf(iPort), sDB, sUser, sPass};
  }
  
  public static
  String getDefaultDbName()
  {
    String sResult = getProperty("nosqldb.dbname");
    if(sResult != null && sResult.length() > 0) {
      return sResult;
    }
    sResult = getProperty("nosqldb.dbauth");
    if(sResult != null && sResult.length() > 0) {
      return sResult;
    }
    sResult = getProperty("nosqldb.uri");
    if(sResult == null || sResult.length() == 0) {
      sResult = getProperty("nosqldb.url");
    }
    String sDbName = "default";
    int iLastSep = sResult != null ? sResult.lastIndexOf('/') : -1;
    if(iLastSep > 0) {
      sDbName = sResult.substring(iLastSep + 1);
      int iSepOpt = sDbName.indexOf('?');
      if(iSepOpt > 0) sDbName = sDbName.substring(0, iSepOpt);
      if(sDbName.equalsIgnoreCase("admin")) sDbName = "default";
    }
    return sDbName;
  }
  
  public static 
  INoSQLDB getDefaultNoSQLDB(String dbName) 
    throws Exception 
  {
    String type = getProperty("nosqldb.type");
    if(type == null || type.length() < 3) {
      type = "mock";
      
      String sUri = getProperty("nosqldb.uri");
      if(sUri == null || sUri.length() == 0) {
        sUri = getProperty("nosqldb.url");
      }
      if(sUri != null && sUri.length() > 0) {
        if(sUri.startsWith("mongo")){
          type = "mongodb";
        }
        else if(sUri.startsWith("http")){
          type = "elasticsearch";
        }
      }
    }
    else {
      type = type.toLowerCase();
    }
    
    INoSQLDB noSQLDB = null;
    
    if (type.startsWith("ela")) {
      noSQLDB = new NoSQLElasticsearch(dbName);
    } 
    else if (type.startsWith("mon")) {
      noSQLDB = new NoSQLMongoDB3(dbName);
    } 
    else {
      noSQLDB = new NoSQLMock(dbName);
    }
    
    noSQLDB.setDebug(DEBUG);
    return noSQLDB;
  }
}
