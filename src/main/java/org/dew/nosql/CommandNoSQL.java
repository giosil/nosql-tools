package org.dew.nosql;

import java.io.File;
import java.io.FileOutputStream;
import java.io.PrintStream;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.dew.nosql.json.JSON;

import org.dew.nosql.util.WUtil;

@SuppressWarnings({"rawtypes","unchecked"})
public 
class CommandNoSQL
{
  protected INoSQLDB noSQLDB;
  protected String dbname;
  protected PrintStream ps;
  protected List<String> listCommands = new ArrayList<String>();
  
  public
  CommandNoSQL(INoSQLDB noSQLDB, String dbname)
    throws Exception
  {
    this.noSQLDB = noSQLDB;
    this.dbname  = dbname;
    if(dbname == null || dbname.length() == 0 || dbname.equalsIgnoreCase("debug")) {
      this.ps = new PrintStream(new File(NoSQLDataSource.getDefaultDbName() + "_nosql.log"));
    }
    else {
      this.ps = new PrintStream(new File(dbname + "_nosql.log"));
    }
  }
  
  public static
  void main(String[] args)
  {
    String sDbName = args != null && args.length > 0 ? args[0] : "";
    
    boolean debug = "debug".equalsIgnoreCase(sDbName);
    if(debug) {
      sDbName = "";
      System.out.println("CommandNoSQL ver. 1.0 (debug mode)");
      System.out.println("----------------------------------");
    }
    else {
      System.out.println("CommandNoSQL ver. 1.0");
      System.out.println("---------------------");
    }
    printHelp();
    INoSQLDB noSQLDB = null;
    try {
      noSQLDB = NoSQLDataSource.getDefaultNoSQLDB(sDbName);
      if(noSQLDB == null) {
        System.err.println("Invalid data source.");
        System.exit(1);
      }
      if(debug) {
        noSQLDB.setDebug(debug);
      }
      
      CommandNoSQL tool = new CommandNoSQL(noSQLDB, sDbName);
      tool.start();
    }
    catch (Exception ex) {
      ex.printStackTrace();
    }
  }
  
  public static
  void printHelp()
  {
    System.out.println("help          = this guide");
    System.out.println("lc            = list commands");
    System.out.println("la            = list aliases");
    System.out.println("l [idx]       = last command [by index]");
    System.out.println("exit          = exit from command sql");
    System.out.println("bye           = exit from command sql");
    System.out.println("col           = list collections");
    System.out.println("ver           = print product version");
    System.out.println("view  <c> {f} = view 20 records of collection c");
    System.out.println("exp   <c> {f} = export collection c");
    System.out.println("find  <c> {f} = find on collection c");
    System.out.println("ins   <c> {d} = insert into collection c");
    System.out.println("upd   <c> {f} {d} = update collection c");
    System.out.println("del   <c> {f} = delete records of collection c");
    System.out.println("file  <f>     = read file f");
    System.out.println("drop  <c>     = drop collection c");
    System.out.println("idx   <c> [f] = create indexes on collection c");
    System.out.println("count <c> [f] = count collection c");
    System.out.println("echo  <?>     = test parameters parsing");
  }
  
  public
  void start()
    throws Exception
  {
    String sLast  = null;
    try {
      do {
        String sCommand = waitForInput();
        
        if(sCommand == null) break;
        if(sCommand.length() == 0) continue;
        if(sCommand.endsWith(";")) {
          sCommand = sCommand.substring(0, sCommand.length()-1);
        }
        if(sCommand.equalsIgnoreCase("exit")) {
          System.out.println("bye");
          ps.println("bye at " + new Date());
          break;
        }
        if(sCommand.equalsIgnoreCase("bye"))  {
          System.out.println("bye");
          ps.println("bye at " + new Date());
          break;
        }
        
        if(sCommand.equalsIgnoreCase("help")) {
          printHelp();
          continue;
        }
        else
        if(sCommand.equalsIgnoreCase("l")) {
          if(sLast != null && sLast.length() > 0) {
            sCommand = sLast;
          }
          else {
            System.out.println("No commands executed");
            continue;
          }
        }
        else
        if(sCommand.startsWith("L ") || sCommand.startsWith("l ")) {
          String sIdx = sCommand.substring(2).trim();
          int iIdx = 0;
          try { iIdx = Integer.parseInt(sIdx); } catch(Exception ex) {}
          if(listCommands != null && listCommands.size() > iIdx) {
            sCommand = listCommands.get(iIdx);
          }
          else {
            System.out.println("Invalid index (" + iIdx + ")");
            continue;
          }
        }
        else
        if(sCommand.equalsIgnoreCase("lc")) {
          if(listCommands != null && listCommands.size() > 0) {
            for(int i = 0; i < listCommands.size(); i++) {
              System.out.println(i + " " + listCommands.get(i));
            }
          }
          continue;
        }
        else
        if(sCommand.equalsIgnoreCase("la")) {
          List aliases = CommandAliases.getAliases();
          if(aliases != null && aliases.size() > 0) {
            for(int i = 0; i < aliases.size(); i++) {
              System.out.println(i + " " + aliases.get(i));
            }
          }
          continue;
        }
        
        if(sCommand.startsWith("$") && sCommand.length() > 1) {
          String sAlias = "";
          String sRight = "";
          int iSep = sCommand.indexOf(' ');
          if(iSep > 0) {
            sAlias = sCommand.substring(0,iSep);
            sRight = sCommand.substring(iSep+1);
          }
          else {
            sAlias = sCommand;
            sRight = "";
          }
          sCommand = CommandAliases.getAlias(sAlias);
          if(sCommand == null || sCommand.length() == 0) {
            System.out.println("alias " + sAlias + " not found");
            continue;
          }
          if(sRight != null && sRight.length() > 0) {
            sCommand += " " + sRight;
          }
        }
        
        ps.println("# " + sCommand);
        if(!sCommand.equals(sLast)) {
          listCommands.add(sCommand);
        }
        sLast = sCommand;
        
        String s3 = sCommand.toLowerCase();
        if(s3.length() > 3) s3 = s3.substring(0, 3);
        
        if(s3.equals("tab") || s3.equals("col")) {
          printCollections();
        }
        else
        if(s3.equals("ver") || s3.equals("inf")) {
          printVersion();
        }
        else
        if(s3.equals("exp")) {
          String sCollection = getCollection(sCommand);
          Map<String,Object> mapFilter = getData(sCommand);
          if(sCollection == null || sCollection.length() == 0) {
            System.out.println("No collection specified.");
            ps.println("No collection specified.");
          }
          else
          if(mapFilter == null) {
            System.out.println("Invalid filter");
            ps.println("Invalid filter");
          }
          else {
            List<Map<String,Object>> listResult = noSQLDB.find(sCollection, mapFilter, "");
            if(listResult != null && listResult.size() > 0) {
              for(int i = 0; i < listResult.size(); i++) {
                System.out.println(JSON.stringify(listResult.get(i)));
                ps.println(JSON.stringify(listResult.get(i)));
              }
              System.out.println(listResult.size() + " records found.");
              ps.println(listResult.size() + " records found.");
            }
            else {
              System.out.println("0 records found.");
              ps.println("0 records found.");
            }
          }
        }
        else
        if(s3.equals("vie")) {
          String sCollection = getCollection(sCommand);
          Map<String,Object> mapFilter = getData(sCommand);
          String sFields = getListString(sCommand);
          if(sCollection == null || sCollection.length() == 0) {
            System.out.println("No collection specified.");
            ps.println("No collection specified.");
          }
          else
          if(mapFilter == null) {
            System.out.println("Invalid filter.");
            ps.println("Invalid filter.");
          }
          else {
            List<Map<String,Object>> listResult = noSQLDB.find(sCollection, mapFilter, sFields, null, 20);
            if(listResult != null && listResult.size() > 0) {
              for(int i = 0; i < listResult.size(); i++) {
                System.out.println(JSON.stringify(listResult.get(i)));
                ps.println(JSON.stringify(listResult.get(i)));
              }
              System.out.println(listResult.size() + " records found.");
              ps.println(listResult.size() + " records found.");
            }
            else {
              System.out.println("0 records found.");
              ps.println("0 records found.");
            }
          }
        }
        else
        if(s3.equals("fin") || s3.equals("sel")) {
          String sCollection = getCollection(sCommand);
          Map<String,Object> mapFilter = getData(sCommand);
          String sFields = getListString(sCommand);
          if(sCollection == null || sCollection.length() == 0) {
            System.out.println("No collection specified.");
            ps.println("No collection specified.");
          }
          else
          if(mapFilter == null) {
            System.out.println("Invalid filter.");
            ps.println("Invalid filter.");
          }
          else {
            List<Map<String,Object>> listResult = noSQLDB.find(sCollection, mapFilter, sFields);
            if(listResult != null && listResult.size() > 0) {
              for(int i = 0; i < listResult.size(); i++) {
                System.out.println(JSON.stringify(listResult.get(i)));
                ps.println(JSON.stringify(listResult.get(i)));
              }
              System.out.println(listResult.size() + " records found.");
              ps.println(listResult.size() + " records found.");
            }
            else {
              System.out.println("0 records found.");
              ps.println("0 records found.");
            }
          }
        }
        else 
        if(s3.equals("upd")) {
          String sCollection = getCollection(sCommand);
          Map<String,Object> mapFilter = getData(sCommand);
          Map<String,Object> mapData   = getData2(sCommand);
          if(sCollection == null || sCollection.length() == 0) {
            System.out.println("No collection specified.");
            ps.println("No collection specified.");
          }
          else
          if(mapFilter == null) {
            System.out.println("Invalid filter.");
            ps.println("Invalid filter.");
          }
          else
          if(mapData == null) {
            System.out.println("Invalid data.");
            ps.println("Invalid data.");
          }
          else
          if(mapData.isEmpty()) {
            System.out.println("Empty data.");
            ps.println("Empty data.");
          }
          else {
            int iResult = noSQLDB.update(sCollection, mapData, mapFilter);
            System.out.println(iResult + " record updated.");
            ps.println(iResult + " record updated.");
          }
        }
        else 
        if(s3.equals("del") || s3.equals("rem")) {
          String sCollection = getCollection(sCommand);
          Map<String,Object> mapFilter = getData(sCommand);
          if(sCollection == null || sCollection.length() == 0) {
            System.out.println("No collection specified.");
            ps.println("No collection specified.");
          }
          else
          if(mapFilter == null) {
            System.out.println("Invalid filter.");
            ps.println("Invalid filter.");
          }
          else {
            int iResult = noSQLDB.delete(sCollection, mapFilter);
            System.out.println(iResult + " record deleted.");
            ps.println(iResult + " record deleted.");
          }
        }
        else 
        if(s3.equals("ins")) {
          String sCollection = getCollection(sCommand);
          Map<String,Object> mapData = getData(sCommand);
          if(sCollection == null || sCollection.length() == 0) {
            System.out.println("No collection specified.");
            ps.println("No collection specified.");
          }
          else
          if(mapData == null) {
            System.out.println("Invalid data.");
            ps.println("Invalid data.");
          }
          else
          if(mapData.isEmpty()) {
            System.out.println("Empty data.");
            ps.println("Empty data.");
          }
          else {
            String sResult = noSQLDB.insert(sCollection, mapData);
            if(sResult != null && sResult.length() > 0) {
              System.out.println(sResult);
              ps.println(sResult);
            }
            else {
              System.out.println("No data inserted.");
              ps.println("No data inserted.");
            }
          }
        }
        else
        if(s3.equals("cou") || s3.equals("cnt")) {
          String sCollection = getCollection(sCommand);
          Map<String,Object> mapFilter = getData(sCommand);
          if(sCollection == null || sCollection.length() == 0) {
            System.out.println("No collection specified.");
            ps.println("No collection specified.");
          }
          else
          if(mapFilter == null) {
            System.out.println("Invalid filter");
            ps.println("Invalid filter");
          }
          else {
            int iResult = noSQLDB.count(sCollection, mapFilter);
            System.out.println(String.valueOf(iResult));
            ps.println(String.valueOf(iResult));
          }
        }
        else 
        if(s3.equals("idx") || s3.equals("ind")) {
          String sCollection = getCollection(sCommand);
          List<String> listFields = getList(sCommand);
          if(sCollection == null || sCollection.length() == 0) {
            System.out.println("No collection specified.");
            ps.println("No collection specified.");
          }
          else {
            if(listFields == null || listFields.size() == 0) {
              List<Map<String,Object>> listIndexes = noSQLDB.listIndexes(sCollection);
              if(listIndexes != null && listIndexes.size() > 0) {
                for(int i = 0; i < listIndexes.size(); i++) {
                  System.out.println(JSON.stringify(listIndexes.get(i)));
                  ps.println(JSON.stringify(listIndexes.get(i)));
                }
                System.out.println(listIndexes.size() + " indexes found.");
                ps.println(listIndexes.size() + " indexes found.");
              }
              else {
                System.out.println("0 indexes found.");
                ps.println("0 indexes found.");
              }
            }
            else {
              for(int i = 0; i < listFields.size(); i++) {
                boolean result = noSQLDB.createIndex(sCollection, listFields.get(0), 1);
                if(result) {
                  System.out.println("Index " + sCollection + "." + listFields.get(0) + " created.");
                  ps.println("Index " + sCollection + "." + listFields.get(0) + " created.");
                }
                else {
                  System.out.println("Index " + sCollection + "." + listFields.get(0) + " NOT created.");
                  ps.println("Index " + sCollection + "." + listFields.get(0) + " NOT created.");
                }
              }
            }
          }
        }
        else
        if(s3.equals("dro")) {
          String sCollection = getCollection(sCommand);
          if(sCollection == null || sCollection.length() == 0) {
            System.out.println("No collection specified.");
            ps.println("No collection specified.");
          }
          else {
            boolean result = noSQLDB.drop(sCollection);
            if(result) {
              System.out.println("Collection " + sCollection + " dropped.");
              ps.println("Collection " + sCollection + " dropped.");
            }
            else {
              System.out.println("Collection " + sCollection + " NOT dropped.");
              ps.println("Collection " + sCollection + " NOT dropped.");
            }
          }
        }
        else
        if(s3.equals("fil")) {
          String sFileName = getCollection(sCommand);
          if(sFileName == null || sFileName.length() == 0) {
            System.out.println("No filename specified.");
            ps.println("No filename specified.");
          }
          else {
            try {
              Map<String,Object> mapFile = noSQLDB.readFile(sFileName);
              if(mapFile == null || mapFile.isEmpty()) {
                System.out.println("File " + sFileName + " not found.");
                ps.println("File " + sFileName + " not found.");
              }
              else {
                byte[] content = WUtil.toArrayOfByte(mapFile.get(INoSQLDB.FILE_CONTENT), false);
                if(content == null || content.length == 0) {
                  System.out.println("Content of " + sFileName + " not available.");
                  ps.println("Content of " + sFileName + " not available.");
                }
                else {
                  String sFilePath = saveContent(content, sFileName);
                  if(sFilePath == null || sFilePath.length() == 0) {
                    System.out.println("File " + sFileName + " not saved.");
                    ps.println("File " + sFileName + " not saved.");
                  }
                  else {
                    System.out.println("File " + sFileName + " saved at " + sFilePath);
                    ps.println("File " + sFileName + " saved at " + sFilePath);
                  }
                }
              }
            }
            catch(Exception ex) {
              System.out.println("Exception: " + ex);
              ps.println("Exception: " + ex);
            }
          }
        }
        else
        if(s3.equals("ech")) {
          String sCollection = getCollection(sCommand);
          Map<String,Object> mapData1 = getData(sCommand);
          Map<String,Object> mapData2 = getData2(sCommand);
          List<String> listFields = getList(sCommand);
          System.out.println("c=" + sCollection + ", d1=" + mapData1 + ", d2=" + mapData2 + ", f=" + listFields);
        }
        else {
          System.out.println("Unknow command.");
          ps.println("Unknow command.");
        }
      }
      while(true);
    }
    catch(Exception ex) {
      ex.printStackTrace();
    }
  }
  
  protected 
  String waitForInput()
  {
    byte[] result = new byte[640];
    int length = 0;
    try {
      System.out.print("# ");
      length = System.in.read(result);
    }
    catch (Exception ex) {
      ex.printStackTrace();
      return null;
    }
    return new String(result, 0, length).trim();
  }
  
  protected
  void printVersion()
  {
    try {
      Map<String,Object> mapInfo = noSQLDB.getInfo();
      if(mapInfo != null && !mapInfo.isEmpty()) {
        System.out.println(JSON.stringify(mapInfo));
      }
      else {
        System.out.println("No info available.");
      }
    }
    catch(Exception ex) {
      System.out.println(ex.getMessage());
      ps.println(ex.getMessage());
    }
  }
  
  protected
  void printCollections()
  {
    try {
      List<String> listCollections = noSQLDB.getCollections();
      if(listCollections != null) {
        int iCount = listCollections.size();
        for(int i = 0; i < iCount; i++) {
          System.out.println(listCollections.get(i));
        }
      }
      else {
        System.out.println("No collections found.");
      }
    }
    catch(Exception ex) {
      System.out.println(ex.getMessage());
      ps.println(ex.getMessage());
    }
  }
  
  protected
  String getCollection(String sCommand)
  {
    if(sCommand == null || sCommand.length() == 0) {
      return null;
    }
    int iStart = sCommand.indexOf("('");
    int iEnd   = -1;
    if(iStart > 0) {
      iEnd = sCommand.indexOf("')", iStart + 1);
    }
    else {
      iStart = sCommand.indexOf("(\"");
      if(iStart > 0) {
        iEnd = sCommand.indexOf("\")", iStart + 1);
      }
      else {
        iStart = sCommand.indexOf(' ');
        if(iStart > 0) {
          iEnd = sCommand.indexOf(' ', iStart + 1);
          if(iEnd < 0) sCommand.indexOf('{', iStart + 1);
          if(iEnd < 0) sCommand.indexOf('[', iStart + 1);
          if(iEnd < 0) sCommand.indexOf('(', iStart + 1);
          if(iEnd < 0) iEnd = sCommand.length();
        }
      }
    }
    if(iStart < 0 || iEnd <= 0) {
      return null;
    }
    String sResult = sCommand.substring(iStart, iEnd).trim();
    if(sResult.startsWith("\"") && sResult.endsWith("\"")) {
      sResult = sResult.substring(1, sResult.length()-1);
    }
    else if(sResult.startsWith("'") && sResult.endsWith("'")) {
      sResult = sResult.substring(1, sResult.length()-1);
    }
    return sResult;
  }
  
  protected
  Map<String,Object> getData(String sCommand)
  {
    if(sCommand == null || sCommand.length() == 0) {
      return new HashMap<String, Object>(0);
    }
    int iStart = sCommand.indexOf('{');
    int iEnd   = -1;
    if(iStart > 0) {
      iEnd = sCommand.indexOf('}', iStart + 1);
    }
    if(iStart < 0 || iEnd <= 0) {
      return new HashMap<String, Object>(0);
    }
    String sData = sCommand.substring(iStart, iEnd + 1);
    Object oResult = null;
    try {
      oResult = JSON.parse(sData);
      if(oResult instanceof Map) {
        return (Map<String,Object>) oResult;
      }
    }
    catch(Exception ex) {
      System.out.println("Invalid JSON " + sData + ": " + ex.getMessage());
    }
    return null;
  }
  
  protected
  Map<String,Object> getData2(String sCommand)
  {
    if(sCommand == null || sCommand.length() == 0) {
      return new HashMap<String, Object>(0);
    }
    int iStart = sCommand.indexOf('{');
    int iEnd   = -1;
    if(iStart > 0) {
      iEnd = sCommand.indexOf('}', iStart + 1);
    }
    if(iStart < 0 || iEnd <= 0) {
      return new HashMap<String, Object>(0);
    }
    
    int iStart2 = sCommand.indexOf('{', iEnd + 1);
    int iEnd2   = -1;
    if(iStart2 > 0) {
      iEnd2 = sCommand.indexOf('}', iStart2 + 1);
    }
    if(iStart2 < 0 || iEnd2 <= 0) {
      return new HashMap<String, Object>(0);
    }
    String sData = sCommand.substring(iStart2, iEnd2 + 1);
    Object oResult = null;
    try {
      oResult = JSON.parse(sData);
      if(oResult instanceof Map) {
        return (Map<String,Object>) oResult;
      }
    }
    catch(Exception ex) {
      System.out.println("Invalid JSON " + sData + ": " + ex.getMessage());
    }
    return null;
  }
  
  protected
  List<String> getList(String sCommand)
  {
    if(sCommand == null || sCommand.length() == 0) {
      return new ArrayList<String>();
    }
    int iStart = sCommand.indexOf('[');
    int iEnd   = -1;
    if(iStart > 0) {
      iEnd = sCommand.indexOf(']', iStart + 1);
    }
    if(iStart < 0 || iEnd <= 0) {
      return new ArrayList<String>();
    }
    String sList = sCommand.substring(iStart, iEnd + 1);
    List<String> listResult = WUtil.toListOfString(sList);
    for(int i = 0; i < listResult.size(); i++) {
      String sItem = listResult.get(i);
      if(sItem.startsWith("\"") && sItem.endsWith("\"")) {
        listResult.set(i, sItem.substring(1,sItem.length()-1));
      }
      else
      if(sItem.startsWith("'") && sItem.endsWith("'")) {
        listResult.set(i, sItem.substring(1,sItem.length()-1));
      }
    }
    return listResult;
  }
  
  protected
  String getListString(String sCommand)
  {
    if(sCommand == null || sCommand.length() == 0) {
      return "";
    }
    int iStart = sCommand.indexOf('[');
    int iEnd   = -1;
    if(iStart > 0) {
      iEnd = sCommand.indexOf(']', iStart + 1);
    }
    if(iStart < 0 || iEnd <= 0) {
      return "";
    }
    String sList = sCommand.substring(iStart, iEnd + 1);
    StringBuilder sbResult = new StringBuilder();
    List<String> listResult = WUtil.toListOfString(sList);
    for(int i = 0; i < listResult.size(); i++) {
      String sItem = listResult.get(i);
      if(sItem.startsWith("\"") && sItem.endsWith("\"")) {
        sbResult.append("," + sItem.substring(1, sItem.length()-1));
      }
      else
      if(sItem.startsWith("'") && sItem.endsWith("'")) {
        sbResult.append("," + sItem.substring(1, sItem.length()-1));
      }
      else {
        sbResult.append("," + sItem);
      }
    }
    if(sbResult.length() > 0) {
      return sbResult.substring(1);
    }
    return "";
  }
  
  public static
  String saveContent(byte[] content, String sFilePath)
    throws Exception
  {
    if(content == null) return null;
    if(content == null || content.length == 0) return null;
    File file = null;
    FileOutputStream fos = null;
    try {
      file = new File(sFilePath);
      fos = new FileOutputStream(sFilePath);
      fos.write(content);
    }
    finally {
      if(fos != null) try{ fos.close(); } catch(Exception ex) {}
    }
    return file.getAbsolutePath();
  }
}