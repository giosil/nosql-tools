package org.dew.nosql;

import java.io.BufferedReader;
import java.io.InputStreamReader;

import java.net.URL;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.sql.Types;

import java.text.SimpleDateFormat;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.dew.nosql.json.JSON;

import org.dew.nosql.util.ListSorter;
import org.dew.nosql.util.WMap;
import org.dew.nosql.util.WUtil;

public
class ReportExecutor
{
  public static
  Map<String,Object> execute(URL url, String fileName)
  {
    if(url == null || fileName == null) {
      return buildDefaultResult(fileName + " not found.");
    }
    String sFileNameLC = fileName.toLowerCase();
    if(sFileNameLC.endsWith(".sql")) {
      return executeSQL(url, new WMap(), null);
    }
    else if(sFileNameLC.endsWith(".nosql")) {
      return executeNoSQL(url, new WMap(), null);
    }
    else if(sFileNameLC.endsWith(".csv")) {
      return executeCSV(url, new WMap());
    }
    else if(sFileNameLC.endsWith(".map") || sFileNameLC.endsWith(".data")) {
      return executeMAP(url, new WMap());
    }
    else if(sFileNameLC.endsWith(".json")) {
      return executeJSON(url, new WMap());
    }
    return buildDefaultResult("File " + fileName + " not executable.");
  }
  
  public static
  Map<String,Object> execute(URL url, String fileName, Map<String,Object> parameters)
  {
    if(url == null || fileName == null) {
      return buildDefaultResult(fileName + " not found.");
    }
    String sFileNameLC = fileName.toLowerCase();
    if(sFileNameLC.endsWith(".sql")) {
      return executeSQL(url, new WMap(parameters), null);
    }
    else if(sFileNameLC.endsWith(".nosql")) {
      return executeNoSQL(url, new WMap(parameters), null);
    }
    else if(sFileNameLC.endsWith(".csv") || sFileNameLC.endsWith(".txt")) {
      return executeCSV(url, new WMap(parameters));
    }
    else if(sFileNameLC.endsWith(".map") || sFileNameLC.endsWith(".data")) {
      return executeMAP(url, new WMap(parameters));
    }
    else if(sFileNameLC.endsWith(".json")) {
      return executeJSON(url, new WMap(parameters));
    }
    return buildDefaultResult("File " + fileName + " not executable.");
  }
  
  public static
  Map<String,Object> buildDefaultResult(String sMessage)
  {
    String[] array0Length = new String[0];
    Map<String,Object> mapResult = new HashMap<String,Object>();
    mapResult.put("success", Boolean.FALSE);
    mapResult.put("message", sMessage);
    mapResult.put("labels",  array0Length);
    mapResult.put("xlabel",  "");
    mapResult.put("ylabels", array0Length);
    mapResult.put("xkey",    "");
    mapResult.put("ykeys",   array0Length);
    mapResult.put("data",    array0Length);
    mapResult.put("rows",    array0Length);
    return mapResult;
  }
  
  public static
  Map<String,Object> executeJSON(URL url, WMap extParams)
  {
    Map<String,Object> mapResult = buildDefaultResult("");
    long lBegin  = System.currentTimeMillis();
    // Parse file
    String sJSONContent  = "";
    String sLine         = null;
    InputStreamReader fr = null;
    BufferedReader br    = null;
    try {
      br = new BufferedReader(fr = new InputStreamReader(url.openStream()));
      while((sLine = br.readLine()) != null) {
        sJSONContent += sLine.trim();
      }
    }
    catch(Exception ex) {
      String sError = ex.getMessage();
      if(sError == null || sError.length() == 0) sError = ex.toString();
      mapResult.put("message", sError);
    }
    finally {
      if(br != null) try{ br.close(); } catch(Exception ex) {}
      if(fr != null) try{ fr.close(); } catch(Exception ex) {}
    }
    if(extParams != null) mapResult.putAll(extParams.toMapObject());
    if(sJSONContent != null && sJSONContent.length() > 0 && sJSONContent.startsWith("{")) {
      Object oMap = JSON.parse(sJSONContent);
      if(oMap instanceof Map) {
        mapResult.putAll(WUtil.toMapObject(oMap));
        long lElapsed = lBegin > 0 ? System.currentTimeMillis() - lBegin : 0l;
        mapResult.put("success", Boolean.TRUE);
        mapResult.put("message", "Report executed in " + lElapsed + " ms");
      }
      else {
        mapResult.put("success", Boolean.FALSE);
        mapResult.put("message", "JSON content is not an object");
      }
    }
    else {
      mapResult.put("success", Boolean.FALSE);
      mapResult.put("message", "JSON content is not valid");
    }
    return mapResult;
  }
  
  public static
  Map<String,Object> executeMAP(URL url, WMap extParams)
  {
    Map<String,Object> mapResult = buildDefaultResult("");
    
    // Valori parametri dinamici
    Calendar cal = Calendar.getInstance();
    long lBegin  = cal.getTimeInMillis();
    int iYYYY    = cal.get(Calendar.YEAR);
    int iXXXX    = cal.get(Calendar.YEAR)  - 1;
    int iWWWW    = cal.get(Calendar.YEAR)  - 2;
    int iMM      = cal.get(Calendar.MONTH) + 1;
    int iDD      = cal.get(Calendar.DATE);
    int iHH      = cal.get(Calendar.HOUR_OF_DAY);
    int iMI      = cal.get(Calendar.MINUTE);
    int iSS      = cal.get(Calendar.SECOND);
    String sYYYY = String.valueOf(iYYYY);
    String sXXXX = String.valueOf(iXXXX);
    String sWWWW = String.valueOf(iWWWW);
    String sMM   = iMM < 10 ? "0" + iMM : String.valueOf(iMM);
    String sDD   = iDD < 10 ? "0" + iDD : String.valueOf(iDD);
    String sHH   = iHH < 10 ? "0" + iHH : String.valueOf(iHH);
    String sMI   = iMI < 10 ? "0" + iMI : String.valueOf(iMI);
    String sSS   = iSS < 10 ? "0" + iSS : String.valueOf(iSS);
    
    // Parse file
    String sJSONContent  = "";
    String sDataObject   = null;
    String sJSONConfig   = "";
    String sSniType      = null;
    String sSnippet      = null;
    boolean boSnippet    = false;
    String sLine         = null;
    InputStreamReader fr = null;
    BufferedReader br    = null;
    try {
      br = new BufferedReader(fr = new InputStreamReader(url.openStream()));
      while((sLine = br.readLine()) != null) {
        if(sLine.startsWith("*/")) {
          boSnippet = false;
          continue;
        }
        if(boSnippet) {
          if(sSnippet == null) sSnippet = "";
          if(sLine.endsWith("*/")) {
            sLine = sLine.substring(0, sLine.length()-2);
            boSnippet = false;
          }
          sSnippet += sLine + "\n";
          continue;
        }
        if(sLine.length() == 0) {
          continue;
        }
        if(sLine.startsWith("/*")) {
          boSnippet = true;
          if(sLine.length() > 2) {
            if(sSnippet == null) sSnippet = "";
            String sFirstRow = sLine.substring(2).trim();
            if(sFirstRow.startsWith("->")) {
              sSniType = sFirstRow.substring(2).trim();
            }
            else {
              sSnippet += sFirstRow + "\n";
            }
          }
          continue;
        }
        sLine = sLine.trim();
        if(sLine.startsWith("--") || sLine.startsWith("//")) {
          if(sLine.length() < 5) continue;
          String sComment = sLine.substring(2).trim();
          if(sComment.startsWith("#")) {
            continue;
          }
          else if(sComment.startsWith("data") || sComment.startsWith("\"data\"")) {
            int iSep1 = sComment.indexOf(':');
            int iSep2 = sComment.indexOf('=');
            if(iSep1 > 0) {
              if(iSep2 > 0 && iSep2 < iSep1) {
                sDataObject = sComment.substring(iSep2+1).trim();
              }
              else {
                sDataObject = sComment.substring(iSep1+1).trim();
              }
            }
            else if(iSep2 > 0) {
              sDataObject = sComment.substring(iSep2+1).trim();
            }
          }
          else if(sComment.indexOf(":") > 0) {
            sJSONConfig += sComment;
          }
          continue;
        }
        sJSONContent += sLine.trim();
      }
    }
    catch(Exception ex) {
      String sError = ex.getMessage();
      if(sError == null || sError.length() == 0) sError = ex.toString();
      mapResult.put("message", sError);
    }
    finally {
      if(br != null) try{ br.close(); } catch(Exception ex) {}
      if(fr != null) try{ fr.close(); } catch(Exception ex) {}
    }
    
    if(!sJSONConfig.startsWith("{")) sJSONConfig = "{" + sJSONConfig + "}";
    if(sJSONConfig.length() > 2) {
      sJSONConfig = sJSONConfig.replace("$YYYY", sYYYY);
      sJSONConfig = sJSONConfig.replace("$XXXX", sXXXX);
      sJSONConfig = sJSONConfig.replace("$WWWW", sWWWW);
      sJSONConfig = sJSONConfig.replace("$MM",   sMM);
      sJSONConfig = sJSONConfig.replace("$DD",   sDD);
      sJSONConfig = sJSONConfig.replace("$HH",   sHH);
      sJSONConfig = sJSONConfig.replace("$MI",   sMI);
      sJSONConfig = sJSONConfig.replace("$SS",   sSS);
      Object oConfig = JSON.parse(sJSONConfig);
      if(oConfig instanceof Map) {
        mapResult.putAll(WUtil.toMapObject(oConfig));
      }
    }
    if(extParams != null) mapResult.putAll(extParams.toMapObject());
    
    String[] asLabels = null;
    List<List<Object>> listRecords  = null;
    List<List<Object>> listData     = null;
    if(sJSONContent != null && sJSONContent.length() > 0 && sJSONContent.startsWith("[")) {
      Object oData = JSON.parse(sJSONContent);
      if(oData instanceof List) {
        listData = WUtil.toListOfListObject(oData);
      }
    }
    else
      if(sJSONContent != null && sJSONContent.length() > 0 && sJSONContent.startsWith("{")) {
        Object oMap = JSON.parse(sJSONContent);
        if(oMap instanceof Map) {
          Map<String,Object> mMap = WUtil.toMapObject(oMap);
          mapResult.putAll(mMap);
          Object oData = mMap.get("data");
          if(oData instanceof List) {
            listData = WUtil.toListOfListObject(oData);
          }
        }
      }
    if(listData == null) listData = new ArrayList<List<Object>>(0);
    listRecords = listData;
    if(listData.size() > 1) {
      Object oFirst = listData.remove(0);
      if(oFirst instanceof List) {
        List<Object> listFirst = WUtil.toListOfObject(oFirst);
        asLabels = new String[listFirst.size()];
        for(int i = 0; i < listFirst.size(); i++) {
          Object oLabel = listFirst.get(i);
          String sLabel = oLabel != null ? oLabel.toString() : String.valueOf(i);
          asLabels[i] = sLabel;
        }
      }
    }
    
    prepareResult(mapResult, asLabels, listRecords, sDataObject, sSniType, sSnippet, lBegin);
    
    return mapResult;
  }
  
  public static
  Map<String,Object> executeCSV(URL url, WMap extParams)
  {
    Map<String,Object> mapResult = buildDefaultResult("");
    
    // Valori parametri dinamici
    Calendar cal = Calendar.getInstance();
    long lBegin  = cal.getTimeInMillis();
    int iYYYY    = cal.get(Calendar.YEAR);
    int iXXXX    = cal.get(Calendar.YEAR)  - 1;
    int iWWWW    = cal.get(Calendar.YEAR)  - 2;
    int iMM      = cal.get(Calendar.MONTH) + 1;
    int iDD      = cal.get(Calendar.DATE);
    int iHH      = cal.get(Calendar.HOUR_OF_DAY);
    int iMI      = cal.get(Calendar.MINUTE);
    int iSS      = cal.get(Calendar.SECOND);
    String sYYYY = String.valueOf(iYYYY);
    String sXXXX = String.valueOf(iXXXX);
    String sWWWW = String.valueOf(iWWWW);
    String sMM   = iMM < 10 ? "0" + iMM : String.valueOf(iMM);
    String sDD   = iDD < 10 ? "0" + iDD : String.valueOf(iDD);
    String sHH   = iHH < 10 ? "0" + iHH : String.valueOf(iHH);
    String sMI   = iMI < 10 ? "0" + iMI : String.valueOf(iMI);
    String sSS   = iSS < 10 ? "0" + iSS : String.valueOf(iSS);
    
    // Parse file
    List<List<Object>> listRecords = new ArrayList<List<Object>>();
    List<Object>       listHeader  = new ArrayList<Object>();
    String sDataObject   = null;
    String sJSONConfig   = "";
    String sSniType      = null;
    String sSnippet      = null;
    boolean boSnippet    = false;
    String sLine         = null;
    InputStreamReader fr = null;
    BufferedReader br    = null;
    try {
      br = new BufferedReader(fr = new InputStreamReader(url.openStream()));
      while((sLine = br.readLine()) != null) {
        if(sLine.startsWith("*/")) {
          boSnippet = false;
          continue;
        }
        if(boSnippet) {
          if(sSnippet == null) sSnippet = "";
          if(sLine.endsWith("*/")) {
            sLine = sLine.substring(0, sLine.length()-2);
            boSnippet = false;
          }
          sSnippet += sLine + "\n";
          continue;
        }
        if(sLine.length() == 0) {
          continue;
        }
        if(sLine.startsWith("/*")) {
          boSnippet = true;
          if(sLine.length() > 2) {
            if(sSnippet == null) sSnippet = "";
            String sFirstRow = sLine.substring(2).trim();
            if(sFirstRow.startsWith("->")) {
              sSniType = sFirstRow.substring(2).trim();
            }
            else {
              sSnippet += sFirstRow + "\n";
            }
          }
          continue;
        }
        sLine = sLine.trim();
        if(sLine.startsWith("--") || sLine.startsWith("//")) {
          if(sLine.length() < 5) continue;
          String sComment = sLine.substring(2).trim();
          if(sComment.startsWith("#")) {
            continue;
          }
          else
            if(sComment.startsWith("data") || sComment.startsWith("\"data\"")) {
              int iSep1 = sComment.indexOf(':');
              int iSep2 = sComment.indexOf('=');
              if(iSep1 > 0) {
                if(iSep2 > 0 && iSep2 < iSep1) {
                  sDataObject = sComment.substring(iSep2+1).trim();
                }
                else {
                  sDataObject = sComment.substring(iSep1+1).trim();
                }
              }
              else if(iSep2 > 0) {
                sDataObject = sComment.substring(iSep2+1).trim();
              }
            }
            else
              if(sComment.indexOf(":") > 0) {
                sJSONConfig += sComment;
              }
          continue;
        }
        sLine = sLine.trim();
        if(sLine.length() > 0) {
          ArrayList<Object> record = new ArrayList<Object>();
          int iIndexOf = 0;
          int iBegin   = 0;
          iIndexOf     = sLine.indexOf(';');
          while(iIndexOf >= 0) {
            record.add(sLine.substring(iBegin, iIndexOf));
            iBegin = iIndexOf + 1;
            iIndexOf = sLine.indexOf(';', iBegin);
          }
          record.add(sLine.substring(iBegin));
          if(listHeader.size() == 0) {
            listHeader.addAll(record);
          }
          else {
            if(record.size() > 1) {
              for(int i = 1; i < record.size(); i++) {
                String sValue = (String) record.get(i);
                char c0 = sValue.length() > 0 ? sValue.charAt(0) : '\0';
                if(Character.isDigit(c0) || c0 == '.' || c0 == ',') {
                  int iSep = sValue.indexOf('.');
                  if(iSep < 0) iSep = sValue.indexOf(',');
                  if(iSep >= 0) {
                    try {
                      record.set(i, new Double(sValue.replace(',', '.')));
                    }
                    catch(Exception ex) {
                    }
                  }
                  else {
                    try {
                      record.set(i, new Integer(sValue));
                    }
                    catch(Exception ex) {
                    }
                  }
                }
              }
            }
            listRecords.add(record);
          }
        }
      }
    }
    catch(Exception ex) {
      String sError = ex.getMessage();
      if(sError == null || sError.length() == 0) sError = ex.toString();
      mapResult.put("message", sError);
    }
    finally {
      if(br != null) try{ br.close(); } catch(Exception ex) {}
      if(fr != null) try{ fr.close(); } catch(Exception ex) {}
    }
    
    if(!sJSONConfig.startsWith("{")) sJSONConfig = "{" + sJSONConfig + "}";
    if(sJSONConfig.length() > 2) {
      sJSONConfig = sJSONConfig.replace("$YYYY", sYYYY);
      sJSONConfig = sJSONConfig.replace("$XXXX", sXXXX);
      sJSONConfig = sJSONConfig.replace("$WWWW", sWWWW);
      sJSONConfig = sJSONConfig.replace("$MM",   sMM);
      sJSONConfig = sJSONConfig.replace("$DD",   sDD);
      sJSONConfig = sJSONConfig.replace("$HH",   sHH);
      sJSONConfig = sJSONConfig.replace("$MI",   sMI);
      sJSONConfig = sJSONConfig.replace("$SS",   sSS);
      Object oConfig = JSON.parse(sJSONConfig);
      if(oConfig instanceof Map) {
        mapResult.putAll(WUtil.toMapObject(oConfig));
      }
    }
    if(extParams != null) mapResult.putAll(extParams.toMapObject());
    String[] asLabels = new String[listHeader.size()];
    for(int i = 0; i < listHeader.size(); i++) {
      asLabels[i] = (String) listHeader.get(i);
    }
    
    prepareResult(mapResult, asLabels, listRecords, sDataObject, sSniType, sSnippet, lBegin);
    
    return mapResult;
  }
  
  public static
  Map<String,Object> executeSQL(URL url, WMap extParams, Connection connection)
  {
    Map<String,Object> mapResult = buildDefaultResult("");
    
    // Valori parametri dinamici
    Calendar cal = Calendar.getInstance();
    long lBegin  = cal.getTimeInMillis();
    int iYYYY    = cal.get(Calendar.YEAR);
    int iXXXX    = cal.get(Calendar.YEAR)  - 1;
    int iWWWW    = cal.get(Calendar.YEAR)  - 2;
    int iMM      = cal.get(Calendar.MONTH) + 1;
    int iDD      = cal.get(Calendar.DATE);
    int iHH      = cal.get(Calendar.HOUR_OF_DAY);
    int iMI      = cal.get(Calendar.MINUTE);
    int iSS      = cal.get(Calendar.SECOND);
    String sYYYY = String.valueOf(iYYYY);
    String sXXXX = String.valueOf(iXXXX);
    String sWWWW = String.valueOf(iWWWW);
    String sMM   = iMM < 10 ? "0" + iMM : String.valueOf(iMM);
    String sDD   = iDD < 10 ? "0" + iDD : String.valueOf(iDD);
    String sHH   = iHH < 10 ? "0" + iHH : String.valueOf(iHH);
    String sMI   = iMI < 10 ? "0" + iMI : String.valueOf(iMI);
    String sSS   = iSS < 10 ? "0" + iSS : String.valueOf(iSS);
    
    // Parse file
    WMap intParams       = new WMap();
    List<String> listSQL = new ArrayList<String>();
    String sDataSource   = null;
    String sDataObject   = null;
    String sJSONConfig   = "";
    String sSniType      = null;
    String sSnippet      = null;
    boolean boSnippet    = false;
    String sSQLCycle     = null;
    String sSQL          = null;
    String sLine         = null;
    InputStreamReader fr = null;
    BufferedReader br    = null;
    try {
      br = new BufferedReader(fr = new InputStreamReader(url.openStream()));
      while((sLine = br.readLine()) != null) {
        if(sLine.startsWith("*/")) {
          boSnippet = false;
          continue;
        }
        if(boSnippet) {
          if(sSnippet == null) sSnippet = "";
          if(sLine.endsWith("*/")) {
            sLine = sLine.substring(0, sLine.length()-2);
            boSnippet = false;
          }
          sSnippet += sLine + "\n";
          continue;
        }
        if(sLine.length() == 0 || sLine.startsWith(";") || sLine.equals("/")) {
          if(sSQL != null && sSQL.length() > 14) listSQL.add(sSQL);
          sSQL = null;
          continue;
        }
        if(sLine.startsWith("set ") || sLine.startsWith("spool ")) {
          continue;
        }
        if(sLine.startsWith("/*")) {
          boSnippet = true;
          if(sLine.length() > 2) {
            if(sSnippet == null) sSnippet = "";
            String sFirstRow = sLine.substring(2).trim();
            if(sFirstRow.startsWith("->")) {
              sSniType = sFirstRow.substring(2).trim();
            }
            else {
              sSnippet += sFirstRow + "\n";
            }
          }
          continue;
        }
        sLine = sLine.trim();
        if(sLine.startsWith("--") || sLine.startsWith("//")) {
          if(sLine.length() < 5) continue;
          String sComment = sLine.substring(2).trim();
          if(sComment.startsWith("#")) {
            continue;
          }
          else if(sComment.startsWith("&")) {
            int iSep = sComment.indexOf('=');
            if(iSep > 1) {
              String sParamName  = sComment.substring(1,iSep);
              String sParamValue = sComment.substring(iSep+1);
              sParamValue = sParamValue.replace("$YYYY", sYYYY);
              sParamValue = sParamValue.replace("$XXXX", sXXXX);
              sParamValue = sParamValue.replace("$WWWW", sWWWW);
              sParamValue = sParamValue.replace("$MM",   sMM);
              sParamValue = sParamValue.replace("$DD",   sDD);
              sParamValue = sParamValue.replace("$HH",   sHH);
              sParamValue = sParamValue.replace("$MI",   sMI);
              sParamValue = sParamValue.replace("$SS",   sSS);
              sParamValue = evalDateExpression(sParamValue);
              intParams.put(sParamName, sParamValue);
            }
          }
          else if(sComment.startsWith("@")) {
            sDataSource = sComment.substring(1).trim();
          }
          else if(sComment.startsWith("SELECT ") || sComment.startsWith("select ") || sComment.startsWith("Select ")) {
            sSQLCycle = sComment;
          }
          else if(sComment.startsWith("data") || sComment.startsWith("\"data\"")) {
            int iSep1 = sComment.indexOf(':');
            int iSep2 = sComment.indexOf('=');
            if(iSep1 > 0) {
              if(iSep2 > 0 && iSep2 < iSep1) {
                sDataObject = sComment.substring(iSep2+1).trim();
              }
              else {
                sDataObject = sComment.substring(iSep1+1).trim();
              }
            }
            else if(iSep2 > 0) {
              sDataObject = sComment.substring(iSep2+1).trim();
            }
          }
          else if(sComment.indexOf(":") > 0) {
            sJSONConfig += sComment;
          }
          continue;
        }
        if(sSQL == null || sSQL.length() == 0) {
          if(!sLine.startsWith("SELECT ") && !sLine.startsWith("select ") && !sLine.startsWith("Select ")) continue;
          sSQL = sLine;
        }
        else {
          if(sLine.endsWith(";")) {
            sSQL += " " + sLine.substring(0, sLine.length() - 1);
            if(sSQL != null && sSQL.length() > 14) listSQL.add(sSQL);
            sSQL = null;
            continue;
          }
          else {
            sSQL += " " + sLine;
          }
        }
      }
      if(sSQL != null && sSQL.length() > 14) listSQL.add(sSQL);
    }
    catch(Exception ex) {
      String sError = ex.getMessage();
      if(sError == null || sError.length() == 0) sError = ex.toString();
      mapResult.put("message", sError);
    }
    finally {
      if(br != null) try{ br.close(); } catch(Exception ex) {}
      if(fr != null) try{ fr.close(); } catch(Exception ex) {}
    }
    if(listSQL == null || listSQL.size() == 0) {
      mapResult.put("message", "SQL not specified");
      return mapResult;
    }
    
    if(!sJSONConfig.startsWith("{")) sJSONConfig = "{" + sJSONConfig + "}";
    if(sJSONConfig.length() > 2) {
      sJSONConfig = sJSONConfig.replace("$YYYY", sYYYY);
      sJSONConfig = sJSONConfig.replace("$XXXX", sXXXX);
      sJSONConfig = sJSONConfig.replace("$WWWW", sWWWW);
      sJSONConfig = sJSONConfig.replace("$MM",   sMM);
      sJSONConfig = sJSONConfig.replace("$DD",   sDD);
      sJSONConfig = sJSONConfig.replace("$HH",   sHH);
      sJSONConfig = sJSONConfig.replace("$MI",   sMI);
      sJSONConfig = sJSONConfig.replace("$SS",   sSS);
      if(sJSONConfig.indexOf('&') > 0) {
        Iterator<Map.Entry<String, Object>> iterator = intParams.toMapObject().entrySet().iterator();
        while(iterator.hasNext()) {
          Map.Entry<String, Object> entry = iterator.next();
          String sKey = (String) entry.getKey();
          String sVal = (String) entry.getValue();
          sJSONConfig = sJSONConfig.replace("&" + sKey, sVal);
        }
      }
      Object oConfig = JSON.parse(sJSONConfig);
      if(oConfig instanceof Map) {
        mapResult.putAll(WUtil.toMapObject(oConfig));
      }
    }
    if(extParams != null) mapResult.putAll(extParams.toMapObject());
    
    // Lettura dei parametri di split
    Object oSplitV  = mapResult.get("split");
    if(oSplitV == null) oSplitV = mapResult.get("split_v");
    if(oSplitV == null) oSplitV = mapResult.get("split_value");
    Object oSplitF  = mapResult.get("split_f");
    if(oSplitF == null) oSplitF = mapResult.get("split_field");
    String sSplitValue = null;
    String sSplitField = null;
    if(oSplitV instanceof String) sSplitValue = (String) oSplitV;
    if(oSplitF instanceof String) sSplitField = (String) oSplitF;
    if(sSplitValue != null && sSplitValue.length() > 0) {
      if(sSplitField == null || sSplitField.length() == 0) {
        sSplitField = sSplitValue;
      }
    }
    
    Map<Object,Object> mapDateFormats = new HashMap<Object,Object>();
    Iterator<String> iterator = extParams.toMapObject().keySet().iterator();
    while(iterator.hasNext()) {
      Object oKey = iterator.next();
      String sKey = oKey.toString();
      Object oFormat = mapResult.get("&" + sKey);
      if(oFormat instanceof String) {
        String sFormat = (String) oFormat;
        if(sFormat.length() > 0) {
          if(sFormat.indexOf('y') >= 0 || sFormat.indexOf('Y') >= 0 || sFormat.indexOf('h') >= 0 || sFormat.indexOf('H') >= 0) {
            mapDateFormats.put(oKey, new SimpleDateFormat(sFormat));
          }
        }
      }
    }
    iterator = intParams.toMapObject().keySet().iterator();
    while(iterator.hasNext()) {
      Object oKey = iterator.next();
      String sKey = oKey.toString();
      Object oFormat = mapResult.get("&" + sKey);
      if(oFormat instanceof String) {
        String sFormat = (String) oFormat;
        if(sFormat.length() > 0) {
          if(sFormat.indexOf('y') >= 0 || sFormat.indexOf('Y') >= 0 || sFormat.indexOf('h') >= 0 || sFormat.indexOf('H') >= 0) {
            mapDateFormats.put(oKey, new SimpleDateFormat(sFormat));
          }
        }
      }
    }
    
    int iMaxRows = WUtil.toInt(mapResult.get("max_rows"), 0);
    if(iMaxRows == 0) iMaxRows = WUtil.toInt(mapResult.get("maxrows"), 0);
    if(iMaxRows != 0) mapResult.put("max_rows", new Integer(iMaxRows));
    int iMaxR = iMaxRows;
    int iMaxLen = WUtil.toInt(mapResult.get("max_len"), 0);
    if(iMaxLen == 0) iMaxLen = WUtil.toInt(mapResult.get("maxlen"), 0);
    
    // Execute
    List<List<Object>> listRecords = null;
    int[] columnTypes      = null;
    String[] asLabels      = null;
    int iCols              = 0;
    Connection conn = null;
    Statement stmC  = null;
    Statement stmV  = null;
    ResultSet rsC   = null;
    ResultSet rsV   = null;
    String sSQLSel1 = null;
    String sSQLSel2 = null;
    try {
      if(connection != null) {
        conn = connection;
      }
      else if(sDataSource != null && sDataSource.length() > 0) {
        conn = ConnectionManager.getConnection(sDataSource);
      }
      else {
        throw new Exception("Datasource not specified");
      }
      stmV = conn.createStatement();
      
      if(sSQLCycle != null && sSQLCycle.length() > 14) {
        sSQLSel1 = replaceParameters(sSQLCycle, extParams, intParams, mapDateFormats);
        stmC = conn.createStatement();
        rsC  = stmC.executeQuery(sSQLSel1);
        ResultSetMetaData rsmdC = rsC.getMetaData();
        int iColsC = rsmdC.getColumnCount();
        
        for(int i = 1; i <= iColsC; i++) {
          Object oFormat = mapResult.get("&" + i);
          if(oFormat instanceof String) {
            String sFormat = (String) oFormat;
            if(sFormat.length() > 0) {
              if(sFormat.indexOf('y') >= 0 || sFormat.indexOf('Y') >= 0 || sFormat.indexOf('h') >= 0 || sFormat.indexOf('H') >= 0) {
                mapDateFormats.put(String.valueOf(i), new SimpleDateFormat(sFormat));
              }
            }
          }
        }
        
        while(rsC.next()) {
          for(int s = 0; s < listSQL.size(); s++) {
            sSQL = listSQL.get(s);
            String sSQLValues = replaceParameters(sSQL, iColsC, rsC, mapDateFormats);
            sSQLSel2 = replaceParameters(sSQLValues, extParams, intParams, mapDateFormats);
            rsV = stmV.executeQuery(sSQLSel2);
            if(columnTypes == null || asLabels == null) {
              ResultSetMetaData rsmdV = rsV.getMetaData();
              iCols = rsmdV.getColumnCount();
              if(iCols == 1 && sSplitValue != null && sSplitValue.length() > 0) {
                columnTypes = new int[2];
                asLabels    = new String[2];
              }
              else {
                columnTypes = new int[iCols];
                asLabels    = new String[iCols];
              }
              // In morris.js labels dovrebbe contenere soltanto le etichette relative a Y,
              // per questo motivo si sposta la X ( getColumnName(1) ) come ultimo elemento.
              if(iCols > 0) {
                asLabels[iCols-1] = rsmdV.getColumnName(1);
                columnTypes[0] = rsmdV.getColumnType(1);
              }
              for(int i = 1; i < iCols; i++) {
                asLabels[i-1]  = rsmdV.getColumnName(i + 1);
                columnTypes[i] = rsmdV.getColumnType(i + 1);
              }
              if(iCols == 1 && sSplitValue != null && sSplitValue.length() > 0) {
                String sField = asLabels[0];
                if(sSplitField != null && sSplitField.length() > 0) {
                  int iSep = sField.indexOf(sSplitField);
                  if(iSep > 0) {
                    asLabels[0] = sField.substring(0,iSep).trim();
                    asLabels[1] = sField.substring(iSep+sSplitField.length()).trim();
                  }
                }
                else {
                  asLabels[1] = "y";
                }
              }
            }
            List<List<Object>> listResult = readResultSet(rsV, iCols, columnTypes, sSplitValue, iMaxR, iMaxLen);
            if(listRecords == null) listRecords = new ArrayList<List<Object>>();
            if(listResult  != null) listRecords.addAll(listResult);
            iMaxR = iMaxR - listResult.size();
            if(iMaxRows > 0 && iMaxR <= 0) break;
          }
          if(iMaxRows > 0 && iMaxR <= 0) break;
        }
      }
      else {
        for(int s = 0; s < listSQL.size(); s++) {
          sSQL = listSQL.get(s);
          
          sSQLSel2 = replaceParameters(sSQL, extParams, intParams, mapDateFormats);
          rsV = stmV.executeQuery(sSQLSel2);
          if(columnTypes == null || asLabels == null) {
            ResultSetMetaData rsmdV = rsV.getMetaData();
            iCols = rsmdV.getColumnCount();
            if(iCols == 1 && sSplitValue != null && sSplitValue.length() > 0) {
              columnTypes = new int[2];
              asLabels    = new String[2];
            }
            else {
              columnTypes = new int[iCols];
              asLabels    = new String[iCols];
            }
            // In morris.js labels dovrebbe contenere soltanto le etichette relative a Y,
            // per questo motivo si sposta la X ( getColumnName(1) ) come ultimo elemento.
            if(iCols > 0) {
              asLabels[iCols-1] = rsmdV.getColumnName(1);
              columnTypes[0]    = rsmdV.getColumnType(1);
            }
            for(int i = 1; i < iCols; i++) {
              asLabels[i-1]  = rsmdV.getColumnName(i + 1);
              columnTypes[i] = rsmdV.getColumnType(i + 1);
            }
            if(iCols == 1 && sSplitValue != null && sSplitValue.length() > 0) {
              String sField = asLabels[0];
              if(sSplitField != null && sSplitField.length() > 0) {
                int iSep = sField.indexOf(sSplitField);
                if(iSep > 0) {
                  asLabels[0] = sField.substring(0,iSep).trim();
                  asLabels[1] = sField.substring(iSep+sSplitField.length()).trim();
                }
              }
              else {
                asLabels[1] = "y";
              }
            }
          }
          List<List<Object>> listResult = readResultSet(rsV, iCols, columnTypes, sSplitValue, iMaxR, iMaxLen);
          if(listRecords == null) listRecords = new ArrayList<List<Object>>();
          if(listResult  != null) listRecords.addAll(listResult);
          iMaxR = iMaxR - listResult.size();
          if(iMaxRows > 0 && iMaxR <= 0) break;
        }
      }
    }
    catch(Exception ex) {
      String sError = ex.getMessage();
      if(sError == null || sError.length() == 0) sError = ex.toString();
      mapResult.put("success", Boolean.FALSE);
      mapResult.put("cycle",   sSQLSel1);
      mapResult.put("sql",     sSQLSel2);
      mapResult.put("message", sError);
      return mapResult;
    }
    finally {
      if(rsV  != null) try{ rsC.close();  } catch(Exception ex) {}
      if(rsC  != null) try{ rsV.close();  } catch(Exception ex) {}
      if(stmV != null) try{ stmV.close(); } catch(Exception ex) {}
      if(stmC != null) try{ stmC.close(); } catch(Exception ex) {}
      if(conn != null && connection == null) {
        try { conn.close(); } catch(Exception ex) {}
      }
    }
    
    prepareResult(mapResult, asLabels, listRecords, sDataObject, sSniType, sSnippet, lBegin);
    
    return mapResult;
  }
  
  public static
  Map<String,Object> executeNoSQL(URL url, WMap extParams, INoSQLDB noSQLDBInstance)
  {
    Map<String,Object> mapResult = buildDefaultResult("");
    
    // Valori parametri dinamici
    Calendar cal = Calendar.getInstance();
    long lBegin  = cal.getTimeInMillis();
    int iYYYY    = cal.get(Calendar.YEAR);
    int iXXXX    = cal.get(Calendar.YEAR)  - 1;
    int iWWWW    = cal.get(Calendar.YEAR)  - 2;
    int iMM      = cal.get(Calendar.MONTH) + 1;
    int iDD      = cal.get(Calendar.DATE);
    int iHH      = cal.get(Calendar.HOUR_OF_DAY);
    int iMI      = cal.get(Calendar.MINUTE);
    int iSS      = cal.get(Calendar.SECOND);
    String sYYYY = String.valueOf(iYYYY);
    String sXXXX = String.valueOf(iXXXX);
    String sWWWW = String.valueOf(iWWWW);
    String sMM   = iMM < 10 ? "0" + iMM : String.valueOf(iMM);
    String sDD   = iDD < 10 ? "0" + iDD : String.valueOf(iDD);
    String sHH   = iHH < 10 ? "0" + iHH : String.valueOf(iHH);
    String sMI   = iMI < 10 ? "0" + iMI : String.valueOf(iMI);
    String sSS   = iSS < 10 ? "0" + iSS : String.valueOf(iSS);
    
    // Parse file
    WMap intParams       = new WMap();
    String sDataObject   = null;
    String sJSONConfig   = "";
    String sSniType      = null;
    String sSnippet      = null;
    boolean boSnippet    = false;
    String sDatabase     = null;
    String sCollection   = null;
    String sFields       = null;
    String sFilter       = null;
    String sOrderBy      = null;
    String sLimit        = null;
    String sLine         = null;
    InputStreamReader fr = null;
    BufferedReader br    = null;
    try {
      br = new BufferedReader(fr = new InputStreamReader(url.openStream()));
      while((sLine = br.readLine()) != null) {
        if(sLine.startsWith("*/")) {
          boSnippet = false;
          continue;
        }
        if(boSnippet) {
          if(sSnippet == null) sSnippet = "";
          if(sLine.endsWith("*/")) {
            sLine = sLine.substring(0, sLine.length()-2);
            boSnippet = false;
          }
          sSnippet += sLine + "\n";
          continue;
        }
        if(sLine.length() == 0 || sLine.startsWith(";") || sLine.equals("/")) {
          continue;
        }
        if(sLine.startsWith("/*")) {
          boSnippet = true;
          if(sLine.length() > 2) {
            if(sSnippet == null) sSnippet = "";
            String sFirstRow = sLine.substring(2).trim();
            if(sFirstRow.startsWith("->")) {
              sSniType = sFirstRow.substring(2).trim();
            }
            else {
              sSnippet += sFirstRow + "\n";
            }
          }
          continue;
        }
        sLine = sLine.trim();
        if(sLine.startsWith("--") || sLine.startsWith("//")) {
          if(sLine.length() < 5) continue;
          String sComment = sLine.substring(2).trim();
          if(sComment.startsWith("#")) {
            continue;
          }
          else if(sComment.startsWith("&")) {
            int iSep = sComment.indexOf('=');
            if(iSep > 1) {
              String sParamName  = sComment.substring(1,iSep);
              String sParamValue = sComment.substring(iSep+1);
              sParamValue = sParamValue.replace("$YYYY", sYYYY);
              sParamValue = sParamValue.replace("$XXXX", sXXXX);
              sParamValue = sParamValue.replace("$WWWW", sWWWW);
              sParamValue = sParamValue.replace("$MM",   sMM);
              sParamValue = sParamValue.replace("$DD",   sDD);
              sParamValue = sParamValue.replace("$HH",   sHH);
              sParamValue = sParamValue.replace("$MI",   sMI);
              sParamValue = sParamValue.replace("$SS",   sSS);
              sParamValue = evalDateExpression(sParamValue);
              intParams.put(sParamName, sParamValue);
            }
          }
          else if(sComment.startsWith("@")) {
            sDatabase = sComment.substring(1).trim();
          }
          else if(sComment.startsWith("$")) {
            sFields = sComment.substring(1).trim();
          }
          else if(sComment.startsWith("^")) {
            sOrderBy = sComment.substring(1).trim();
          }
          else if(sComment.startsWith("data") || sComment.startsWith("\"data\"")) {
            int iSep1 = sComment.indexOf(':');
            int iSep2 = sComment.indexOf('=');
            if(iSep1 > 0) {
              if(iSep2 > 0 && iSep2 < iSep1) {
                sDataObject = sComment.substring(iSep2+1).trim();
              }
              else {
                sDataObject = sComment.substring(iSep1+1).trim();
              }
            }
            else if(iSep2 > 0) {
              sDataObject = sComment.substring(iSep2+1).trim();
            }
          }
          else if(sComment.indexOf(":") > 0) {
            sJSONConfig += sComment;
          }
          continue;
        }
        sLine = sLine.toLowerCase();
        if(sLine.startsWith("select ")) {
          int iFrom = sLine.indexOf("from ");
          if(iFrom > 0) {
            int iEndFrom = -1; 
            int iWhere   = sLine.indexOf("where ");
            if(iWhere > 0) iEndFrom = iWhere;
            int iOrderBy = sLine.indexOf("order by");
            if(iEndFrom < 0) iEndFrom = iOrderBy;
            int iGroupBy = sLine.indexOf("group by");
            if(iEndFrom < 0) iEndFrom = iGroupBy;
            if(iEndFrom < 0) iEndFrom = sLine.length();
            
            if(iWhere > 0) {
              if(iOrderBy > 0) {
                sFilter = sLine.substring(iWhere+6,iOrderBy).trim();
              }
              else if(iGroupBy > 0) {
                sFilter = sLine.substring(iWhere+6,iGroupBy).trim();
              }
              else {
                sFilter = sLine.substring(iWhere+6).trim();
              }
              sFilter = sFilter.replace(">=", ">=:").replace("<=", "<=:").replace("<>", "<>:").replace(">", ">:").replace("<", "<:").replace("=", ":");
              sFilter = sFilter.replace(" and ", ",").replace(" or ", ",").replace("(", "").replace(")", "");
              sFilter = sFilter.replace(" is null", ":null").replace(" is not null ", "<>:null");
            }
            
            sCollection = sLine.substring(iFrom+5, iEndFrom).trim();
            int iSep = sCollection.indexOf(',');
            if(iSep > 0) sCollection = sCollection.substring(0, iSep);
            
            sFields = sLine.substring(7, iFrom).trim();
            
            if(iOrderBy > 0) {
              sOrderBy = sLine.substring(iOrderBy+8).trim();
            }
          }
        }
        else {
          int iFind = sLine.indexOf(".find(");
          if(iFind > 0) {
            sCollection  = sLine.substring(0, iFind).trim();
            int iEndFind = sLine.indexOf(")", iFind+1);
            if(iEndFind > 0) {
              sFilter  = sLine.substring(iFind+6, iEndFind).trim();
            }
            int iSort = sLine.indexOf(".sort(");
            if(iSort > 0) {
              int iEndSort = sLine.indexOf(")", iSort+1);
              if(iEndSort > 0) {
                sOrderBy = sLine.substring(iSort+6, iEndSort).trim();
                sOrderBy = sOrderBy.replace("{", "").replace("}", "").replace("[", "").replace("]", "").replace(" ", "");
                sOrderBy = sOrderBy.replace("\"", "").replace("'", "").replace(":1", " 1").replace(":-1", " -1").replace(":", "");
              }
            }
            int iProjection = sLine.indexOf(".projection(");
            if(iProjection > 0) {
              int iEndProjection = sLine.indexOf(")", iProjection+1);
              if(iEndProjection > 0) {
                sFields = sLine.substring(iProjection+12, iEndProjection).trim();
                sFields = sFields.replace("{", "").replace("}", "").replace("[", "").replace("]", "").replace(" ", "");
                sFields = sFields.replace("\"", "").replace("'", "").replace(":1", "").replace(":", "");
              }
            }
            int iLimit = sLine.indexOf(".limit(");
            if(iLimit > 0) {
              int iEndLimit = sLine.indexOf(")", iLimit+1);
              if(iEndLimit > 0) {
                sLimit = sLine.substring(iLimit+7, iEndLimit).trim();
              }
            }
          }
          else {
            sCollection = sLine;
          }
          if(sCollection.startsWith("db.")) {
            sCollection = sCollection.substring(3).trim();
          }
        }
      }
    }
    catch(Exception ex) {
      String sError = ex.getMessage();
      if(sError == null || sError.length() == 0) sError = ex.toString();
      mapResult.put("message", sError);
    }
    finally {
      if(br != null) try{ br.close(); } catch(Exception ex) {}
      if(fr != null) try{ fr.close(); } catch(Exception ex) {}
    }
    if(sCollection == null || sCollection.length() == 0) {
      mapResult.put("message", "Collection not specified");
      return mapResult;
    }
    
    if(!sJSONConfig.startsWith("{")) sJSONConfig = "{" + sJSONConfig + "}";
    if(sJSONConfig.length() > 2) {
      sJSONConfig = sJSONConfig.replace("$YYYY", sYYYY);
      sJSONConfig = sJSONConfig.replace("$XXXX", sXXXX);
      sJSONConfig = sJSONConfig.replace("$WWWW", sWWWW);
      sJSONConfig = sJSONConfig.replace("$MM",   sMM);
      sJSONConfig = sJSONConfig.replace("$DD",   sDD);
      sJSONConfig = sJSONConfig.replace("$HH",   sHH);
      sJSONConfig = sJSONConfig.replace("$MI",   sMI);
      sJSONConfig = sJSONConfig.replace("$SS",   sSS);
      if(sJSONConfig.indexOf('&') > 0) {
        Iterator<Map.Entry<String, Object>> iterator = intParams.toMapObject().entrySet().iterator();
        while(iterator.hasNext()) {
          Map.Entry<String, Object> entry = iterator.next();
          String sKey = (String) entry.getKey();
          String sVal = (String) entry.getValue();
          sJSONConfig = sJSONConfig.replace("&" + sKey, sVal);
        }
      }
      Map<String,Object> mapConfig = JSON.parseObj(sJSONConfig);
      if(mapConfig != null) mapResult.putAll(mapConfig);
    }
    if(extParams != null) mapResult.putAll(extParams.toMapObject());
    
    // Lettura dei parametri di split
    Object oSplitV  = mapResult.get("split");
    if(oSplitV == null) oSplitV = mapResult.get("split_v");
    if(oSplitV == null) oSplitV = mapResult.get("split_value");
    Object oSplitF  = mapResult.get("split_f");
    if(oSplitF == null) oSplitF = mapResult.get("split_field");
    String sSplitValue = null;
    String sSplitField = null;
    if(oSplitV instanceof String) sSplitValue = (String) oSplitV;
    if(oSplitF instanceof String) sSplitField = (String) oSplitF;
    if(sSplitValue != null && sSplitValue.length() > 0) {
      if(sSplitField == null || sSplitField.length() == 0) {
        sSplitField = sSplitValue;
      }
    }
    
    Map<Object,Object> mapDateFormats = new HashMap<Object,Object>();
    Iterator<String> iterator = extParams.toMapObject().keySet().iterator();
    while(iterator.hasNext()) {
      Object oKey = iterator.next();
      String sKey = oKey.toString();
      Object oFormat = mapResult.get("&" + sKey);
      if(oFormat instanceof String) {
        String sFormat = (String) oFormat;
        if(sFormat.length() > 0) {
          if(sFormat.indexOf('y') >= 0 || sFormat.indexOf('Y') >= 0 || sFormat.indexOf('h') >= 0 || sFormat.indexOf('H') >= 0) {
            mapDateFormats.put(oKey, new SimpleDateFormat(sFormat));
          }
        }
      }
    }
    iterator = intParams.toMapObject().keySet().iterator();
    while(iterator.hasNext()) {
      Object oKey = iterator.next();
      String sKey = oKey.toString();
      Object oFormat = mapResult.get("&" + sKey);
      if(oFormat instanceof String) {
        String sFormat = (String) oFormat;
        if(sFormat.length() > 0) {
          if(sFormat.indexOf('y') >= 0 || sFormat.indexOf('Y') >= 0 || sFormat.indexOf('h') >= 0 || sFormat.indexOf('H') >= 0) {
            mapDateFormats.put(oKey, new SimpleDateFormat(sFormat));
          }
        }
      }
    }
    
    int iMaxRows = WUtil.toInt(mapResult.get("max_rows"), 0);
    if(iMaxRows == 0) iMaxRows = WUtil.toInt(mapResult.get("maxrows"), 0);
    if(iMaxRows != 0) mapResult.put("max_rows", new Integer(iMaxRows));
    int iMaxLen = WUtil.toInt(mapResult.get("max_len"), 0);
    if(iMaxLen == 0) iMaxLen = WUtil.toInt(mapResult.get("maxlen"), 0);
    if(sLimit != null && sLimit.length() > 0) {
      int iLimitRows = WUtil.toInt(sLimit, 0);
      if(iLimitRows > 0) iMaxRows = iLimitRows;
    }
    
    List<String> listFields = new ArrayList<String>();
    if(sFields != null && sFields.length() > 0) {
      int iIndexOf = 0;
      int iBegin   = 0;
      iIndexOf     = sFields.indexOf(',');
      while(iIndexOf >= 0) {
        listFields.add(sFields.substring(iBegin, iIndexOf).trim());
        iBegin = iIndexOf + 1;
        iIndexOf = sFields.indexOf(',', iBegin);
      }
      listFields.add(sFields.substring(iBegin).trim());
    }
    
    // Execute
    List<List<Object>> listRecords = new ArrayList<List<Object>>();
    String[] asLabels = null;
    INoSQLDB noSQLDB  = noSQLDBInstance;
    try {
      if(noSQLDB == null) {
        noSQLDB = ConnectionManager.getDefaultNoSQLDB(sDatabase);
      }
      
      Map<String,Object> mapFilter = null;
      if(sFilter == null || sFilter.length() < 3) {
        mapFilter = new HashMap<String,Object>();
      }
      else {
        sFilter = replaceFilterParameters(sFilter, extParams, intParams, mapDateFormats);
        
        if(!sFilter.startsWith("{")) sFilter = "{" + sFilter + "}";
        try { mapFilter = JSON.parseObj(sFilter); } catch(Exception ex) {}
        if(mapFilter == null) {
          mapFilter = new HashMap<String,Object>(); 
        }
      }
      if(extParams != null) {
        Map<String,Object> mapFilterExt = new HashMap<String,Object>(extParams.toMapObject());
        mapFilterExt.remove("result");
        mapFilterExt.remove("max_rows");
        mapFilterExt.remove("max_len");
        mapFilterExt.remove("perc");
        mapFilterExt.remove("calc");
        mapFilterExt.remove("sort");
        mapFilterExt.remove("$1");
        mapFilterExt.remove("$2");
        mapFilter.putAll(mapFilterExt);
      }
      
      List<Map<String,Object>> listFindResult = noSQLDB.find(sCollection, mapFilter, sFields, sOrderBy, iMaxRows);
      
      if(listFindResult != null && listFindResult.size() > 0) {
        Set<String> keys = new HashSet<String>();
        Map<String,Object> item0 = listFindResult.get(0);
        Map<String,Object> itemL = listFindResult.get(listFindResult.size()-1);
        keys.addAll(item0.keySet());
        keys.addAll(itemL.keySet());
        
        Iterator<String> itKeys = keys.iterator();
        while(itKeys.hasNext()) {
          String key = itKeys.next();
          Object val = item0.get(key);
          if(val == null) val = itemL.get(key);
          if(listFields.contains(key)) continue;
          if(key.equals("_id")) continue;
          if(val instanceof Number) {
            listFields.add(key);
          }
          else {
            listFields.add(0, key);
          }
        }
        
        // In morris.js labels dovrebbe contenere soltanto le etichette relative a Y,
        // per questo motivo si sposta la X (0) come ultimo elemento.
        asLabels = new String[listFields.size()];
        if(listFields.size() > 0) {
          asLabels[listFields.size()-1] = listFields.get(0);
        }
        for(int j = 1; j < listFields.size(); j++) {
          asLabels[j-1] = listFields.get(j);
        }
        
        Object lastXValue = null;
        List<Object> lastRecord = null;
        for(int i = 0; i < listFindResult.size(); i++) {
          Map<String,Object> mapRecord = listFindResult.get(i);
          List<Object> listRecord = new ArrayList<Object>(listFields.size());
          for(int j = 0; j < listFields.size(); j++) {
            listRecord.add(mapRecord.get(listFields.get(j)));
          }
          if(listRecord.size() > 0) {
            Object xValue = listRecord.get(0);
            if(xValue != null && lastXValue != null && lastRecord != null && xValue.equals(lastXValue)) {
              for(int j = 1; j < lastRecord.size(); j++) {
                lastRecord.set(j, sum(lastRecord.get(j), listRecord.get(j)));
              }
              continue;
            }
            lastXValue = xValue;
          }
          listRecords.add(listRecord);
          lastRecord = listRecord;
        }
      }
    }
    catch(Exception ex) {
      String sError = ex.getMessage();
      if(sError == null || sError.length() == 0) sError = ex.toString();
      mapResult.put("success",    Boolean.FALSE);
      mapResult.put("database",   sDatabase);
      mapResult.put("collection", sCollection);
      mapResult.put("message",    sError);
      return mapResult;
    }
    
    prepareResult(mapResult, asLabels, listRecords, sDataObject, sSniType, sSnippet, lBegin);
    
    return mapResult;
  }
  
  private static
  void prepareResult(Map<String,Object> mapResult, String[] asLabels, List<List<Object>> listRecords, String sDataObject, String sConType, String sSnippet, long lBegin)
  {
    long lElapsed = lBegin > 0 ? System.currentTimeMillis() - lBegin : 0l;
    try {
      if(asLabels    == null) asLabels    = new String[0];
      if(listRecords == null) listRecords = new ArrayList<List<Object>>(0);
      
      boolean boPercentage = WUtil.toBoolean(mapResult.get("perc"), false);
      if(boPercentage) calcPercentages(listRecords);
      
      boolean boCalculate = WUtil.toBoolean(mapResult.get("calc"), false);
      if(boCalculate) {
        double[] adMinMaxSumAvg = calcMinMaxSumAvg(listRecords);
        if(adMinMaxSumAvg != null && adMinMaxSumAvg.length > 3) {
          mapResult.put("min", new Double(adMinMaxSumAvg[0]));
          mapResult.put("max", new Double(adMinMaxSumAvg[1]));
          mapResult.put("sum", new Double(adMinMaxSumAvg[2]));
          mapResult.put("avg", new Double(adMinMaxSumAvg[3]));
        }
      }
      
      // replace labels
      for(int i = 1; i <= asLabels.length; i++) {
        Object oLabel = mapResult.remove("$l" + i);
        if(oLabel instanceof String) {
          String sLabel = (String) oLabel;
          if(sLabel != null && sLabel.length() > 0) asLabels[i-1] = sLabel;
        }
      }
      // labels -> keys
      String   sXKey   = null;
      String[] asYKeys = null;
      if(asLabels.length == 0) {
        sXKey   = "";
        asYKeys = new String[0];
      }
      else
        if(asLabels.length > 0) {
          sXKey   = asLabels[0];
          asYKeys = new String[asLabels.length-1];
          Object oXKey = mapResult.remove("$1");
          if(oXKey == null) oXKey = mapResult.remove("$x");
          if(oXKey instanceof String) {
            String sKey = (String) oXKey;
            if(sKey != null && sKey.length() > 0) sXKey = sKey;
          }
          for(int i = 2; i <= asLabels.length; i++) {
            asYKeys[i-2] = asLabels[i-1];
            Object oKey = mapResult.remove("$" + i);
            if(oKey instanceof String) {
              String sKey = (String) oKey;
              if(sKey != null && sKey.length() > 0) asYKeys[i-2] = sKey;
            }
          }
        }
      
      int iSort = WUtil.toInt(mapResult.get("sort"), 0);
      if(iSort != 0) {
        boolean boDec = iSort < 0;
        int iField = boDec ?(iSort*-1)-1 : iSort-1;
        if(listRecords.size() > 0 && iField < asLabels.length) {
          ListSorter.sortListOfList(listRecords, iField);
          if(boDec) listRecords = ListSorter.reverse(listRecords);
        }
      }
      
      Object oResult  = mapResult.get("result");
      int iResultType = getResultType(oResult);
      
      mapResult.put("success", Boolean.TRUE);
      mapResult.put("message", "Report executed in " + lElapsed + " ms");
      mapResult.put("labels",  asLabels);
      if(asLabels.length == 1) {
        mapResult.put("xlabel",  asLabels[0]);
      }
      else if(asLabels.length > 1) {
        mapResult.put("xlabel",  asLabels[0]);
        mapResult.put("ylabels", Arrays.copyOfRange(asLabels, 1, asLabels.length));
      }
      mapResult.put("xkey",    sXKey);
      mapResult.put("ykeys",   asYKeys);
      mapResult.put("rows",    new Integer(listRecords.size()));
      switch(iResultType) {
      case 1:  mapResult.put("data", toArrayOfMap(listRecords, sXKey, asYKeys)); break;
      case 2:  mapResult.put("data", toMapOfArray(listRecords, sXKey, asYKeys)); break;
      case 3:
        mapResult.put("xdata", getXValues(listRecords));
        mapResult.put("data",  getYValues(listRecords));
        break;
      default: mapResult.put("data", listRecords); break;
      }
      
      if(sDataObject != null && sDataObject.length() > 1) {
        int iData = sDataObject.indexOf("%data%");
        if(iData >= 0) {
          Object oData = mapResult.get("data");
          String sData = JSON.stringify(oData);
          sDataObject = sDataObject.replace("%data%", sData);
          oData = JSON.parse(sDataObject);
          mapResult.put("data", oData);
        }
      }
      if(sSnippet != null && sSnippet.length() > 1) {
        Iterator<Map.Entry<String, Object>> iterator = mapResult.entrySet().iterator();
        while(iterator.hasNext()) {
          Map.Entry<String, Object> entry = iterator.next();
          String sKey  = entry.getKey().toString();
          int iIndexOf = sSnippet.indexOf("%" + sKey + "%");
          if(iIndexOf >= 0) {
            Object oVal = entry.getValue();
            String sVal = JSON.stringify(oVal);
            sSnippet  = sSnippet.replace("%" + sKey + "%", sVal);
          }
        }
        mapResult.put("snippetType", sSnippet);
        mapResult.put("snippet",     sSnippet);
      }
    }
    catch(Exception ex) {
      String sError = ex.getMessage();
      if(sError == null || sError.length() == 0) sError = ex.toString();
      mapResult.put("success", Boolean.FALSE);
      mapResult.put("message", sError);
    }
  }
  
  private static
  String replaceParameters(String sSQL, WMap extParams, WMap intParams, Map<Object,Object> mapDateFormats)
  {
    Iterator<String> iterator = extParams.toMapObject().keySet().iterator();
    while(iterator.hasNext()) {
      Object oKey = iterator.next();
      String sVal = null;
      SimpleDateFormat df = (SimpleDateFormat) mapDateFormats.get(oKey);
      if(df != null) {
        Date dValue = extParams.getDate(oKey);
        if(dValue != null) {
          sVal = df.format(dValue);
        }
        else {
          sVal = df.format(new java.util.Date());
        }
      }
      else {
        sVal = extParams.getString(oKey);
        if(sVal != null && sVal.length() > 0) {
          sVal = sVal.replace("'", "''");
        }
      }
      sSQL = sSQL.replace("&" + oKey, sVal);
    }
    iterator = intParams.toMapObject().keySet().iterator();
    while(iterator.hasNext()) {
      Object oKey = iterator.next();
      String sVal = null;
      SimpleDateFormat df = (SimpleDateFormat) mapDateFormats.get(oKey);
      if(df != null) {
        Date dValue = intParams.getDate(oKey);
        if(dValue != null) {
          sVal = df.format(dValue);
        }
        else {
          sVal = df.format(new java.util.Date());
        }
      }
      else {
        sVal = intParams.getString(oKey);
        if(sVal != null && sVal.length() > 0) {
          sVal = sVal.replace("'", "''");
        }
      }
      sSQL = sSQL.replace("&" + oKey, sVal);
    }
    // Rimozione parametri non impostati
    int iStart = 0;
    int iPar   = sSQL.indexOf('&', iStart);
    while(iPar >= 0) {
      int[] aiBegEnd = findClause(sSQL, iPar);
      if(aiBegEnd != null && aiBegEnd.length > 1) {
        String sSQL1 = sSQL.substring(0, aiBegEnd[0]);
        String sSQL2 = null;
        if(aiBegEnd[1] > 0) {
          sSQL2 = sSQL.substring(aiBegEnd[1]);
        }
        else {
          sSQL2 = "";
        }
        sSQL = sSQL1 + sSQL2;
      }
      iStart = iPar;
      if(iStart >= sSQL.length()) break;
      iPar = sSQL.indexOf('&', iStart+1);
    }
    return sSQL;
  }
  
  private static
  String replaceParameters(String sSQL, int iCols, ResultSet rs, Map<Object,Object> mapDateFormats)
    throws Exception
  {
    for(int i = 1; i <= iCols; i++) {
      String sVal = null;
      SimpleDateFormat df = (SimpleDateFormat) mapDateFormats.get(String.valueOf(i));
      if(df != null) {
        Date dValue = rs.getDate(i);
        if(dValue != null) {
          sVal = df.format(dValue);
        }
        else {
          sVal = df.format(new java.util.Date());
        }
      }
      else {
        sVal = rs.getString(i);
        if(sVal != null && sVal.length() > 0) {
          sVal = sVal.replace("'", "''");
        }
      }
      sSQL = sSQL.replace("&" + i, sVal);
    }
    return sSQL;
  }
  
  private static
  String replaceFilterParameters(String sFilter, WMap extParams, WMap intParams, Map<Object,Object> mapDateFormats)
  {
    if(sFilter == null || sFilter.length() == 0) {
      return sFilter;
    }
    Iterator<String> iterator = extParams.toMapObject().keySet().iterator();
    while(iterator.hasNext()) {
      Object oKey = iterator.next();
      String sVal = null;
      SimpleDateFormat df = (SimpleDateFormat) mapDateFormats.get(oKey);
      if(df != null) {
        Date dValue = extParams.getDate(oKey);
        if(dValue != null) {
          sVal = df.format(dValue);
        }
        else {
          sVal = df.format(new java.util.Date());
        }
      }
      else {
        sVal = extParams.getString(oKey);
      }
      sFilter = sFilter.replace("&" + oKey, sVal);
    }
    iterator = intParams.toMapObject().keySet().iterator();
    while(iterator.hasNext()) {
      Object oKey = iterator.next();
      String sVal = null;
      SimpleDateFormat df = (SimpleDateFormat) mapDateFormats.get(oKey);
      if(df != null) {
        Date dValue = intParams.getDate(oKey);
        if(dValue != null) {
          sVal = df.format(dValue);
        }
        else {
          sVal = df.format(new java.util.Date());
        }
      }
      else {
        sVal = intParams.getString(oKey);
      }
      sFilter = sFilter.replace("&" + oKey, sVal);
    }
    return sFilter;
  }
  
  private static
  List<List<Object>> readResultSet(ResultSet rs, int iCols, int[] columnTypes, String sSplitValue, int iMaxRows, int iMaxLen)
    throws Exception
  {
    List<List<Object>> listRecords = new ArrayList<List<Object>>();
    try {
      int iRows = 0;
      while(rs.next()) {
        List<Object> listRecord = new ArrayList<Object>(iCols);
        for(int i = 0; i < iCols; i++) {
          int iType = columnTypes[i];
          switch(iType) {
          case Types.BIT:
          case Types.TINYINT:
          case Types.SMALLINT:
          case Types.INTEGER:
          case Types.BIGINT:
            int iValue = rs.getInt(i + 1);
            listRecord.add(new Integer(iValue));
            break;
          case Types.FLOAT:
          case Types.REAL:
          case Types.DOUBLE:
          case Types.NUMERIC:
          case Types.DECIMAL:
            double dValue = rs.getDouble(i + 1);
            listRecord.add(new Double(dValue));
            break;
          case Types.DATE:
          case Types.TIME:
            Date dtValue  = rs.getDate(i + 1);
            listRecord.add(dtValue);
            break;
          case Types.TIMESTAMP:
            Date tsValue = rs.getTimestamp(i + 1);
            listRecord.add(tsValue);
            break;
          default:
            String sValue = rs.getString(i + 1);
            if(sValue == null) sValue = "";
            if(iCols == 1 && sSplitValue != null && sSplitValue.length() > 0) {
              int iSep = sValue.indexOf(sSplitValue);
              if(iSep > 0) {
                String s1 = sValue.substring(0, iSep).trim();
                if(iMaxLen > 0) {
                  if(s1.length() > iMaxLen) {
                    listRecord.add(s1.substring(0, iMaxLen));
                  }
                  else {
                    listRecord.add(s1);
                  }
                }
                else {
                  listRecord.add(s1);
                }
                String s2 = sValue.substring(iSep + sSplitValue.length()).trim();
                if(s2.endsWith(sSplitValue)) {
                  s2 = s2.substring(0, s2.length()-sSplitValue.length());
                }
                try {
                  double d2 = Double.parseDouble(s2);
                  listRecord.add(new Double(d2));
                }
                catch(Throwable th) {
                  listRecord.add(s2);
                }
              }
              else {
                if(iMaxLen > 0) {
                  if(sValue.length() > iMaxLen) {
                    listRecord.add(sValue.substring(0, iMaxLen));
                  }
                  else {
                    listRecord.add(sValue);
                  }
                }
                else {
                  listRecord.add(sValue);
                }
                listRecord.add(new Integer(0));
              }
            }
            else {
              if(iMaxLen > 0) {
                if(sValue.length() > iMaxLen) {
                  listRecord.add(sValue.substring(0, iMaxLen));
                }
                else {
                  listRecord.add(sValue);
                }
              }
              else {
                listRecord.add(sValue);
              }
            }
            break;
          }
        }
        listRecords.add(listRecord);
        iRows++;
        if(iMaxRows > 0 && iRows >= iMaxRows) break;
      }
    }
    finally {
      if(rs != null) try{ rs.close(); } catch(Exception ex) {}
    }
    return listRecords;
  }
  
  /**
   * I risultati possono essere restituiti in quattro modi diversi combinando array/list e map/object.
   *
   * 0) array of array (aa/ll):     [[0,1],[1,2],[2,3]]
   * 1) array of map   (am/ao/lo):  [{"x":0,"y":1},{"x":1,"y":2},{"x":2,"y":3}]
   * 2) map   of array (ma/oa/ol):  {"x":[0,1,2],"y":[1,2,3]}
   * 3) separated  xdata=[0,1,2], data=[1,2,3]
   *
   * @param oResult (def=1)
   * @return
   */
  private static
  int getResultType(Object oResult)
  {
    if(oResult == null) return 1;
    if(oResult instanceof Number) {
      return ((Number) oResult).intValue();
    }
    String sResult = oResult.toString();
    if(sResult.startsWith("s")) return 3;
    if(sResult.length()  <   2) return 1;
    sResult = sResult.toLowerCase();
    if(sResult.length() == 2) {
      if(sResult.equals("aa") || sResult.equals("ll")) return 0;
      if(sResult.equals("am") || sResult.equals("ao") || sResult.equals("lo")) return 1;
      if(sResult.equals("ma") || sResult.equals("oa") || sResult.equals("ol")) return 2;
      if(sResult.equals("se")) return 3;
      return 1;
    }
    int iArray = sResult.indexOf("ar");
    if(iArray < 0) iArray = sResult.indexOf("li");
    int iMap   = sResult.indexOf("ma");
    if(iMap < 0) iMap = sResult.indexOf("ob");
    if(iMap < 0) return 0;
    if(iArray < iMap) return 1;
    if(iArray > iMap) return 2;
    return 1;
  }
  
  private static
  List<Map<String,Object>> toArrayOfMap(List<List<Object>> listOfList, String sXKey, String[] asYKeys)
  {
    if(listOfList == null || listOfList.size() == 0) return new ArrayList<Map<String,Object>>(0);
    List<Map<String,Object>> listResult = new ArrayList<Map<String,Object>> (listOfList.size());
    for(int i = 0; i < listOfList.size(); i++) {
      List<Object> record = listOfList.get(i);
      Map<String,Object>  map  = new HashMap<String,Object>(asYKeys.length + 1);
      map.put(sXKey, record.get(0));
      for(int j = 0; j < asYKeys.length; j++) {
        map.put(asYKeys[j], record.get(j+1));
      }
      listResult.add(map);
    }
    return listResult;
  }
  
  private static
  Map<String, List<Object>> toMapOfArray(List<List<Object>> listOfList, String sXKey, String[] asYKeys)
  {
    if(listOfList == null || listOfList.size() == 0) return new HashMap<String, List<Object>> (0);
    Map<String, List<Object>> mapResult = new HashMap<String, List<Object>>(asYKeys.length + 1);
    List<Object> list0 = new ArrayList<Object>(listOfList.size());
    for(int i = 0; i < listOfList.size(); i++) {
      List<Object> record = listOfList.get(i);
      list0.add(record.get(0));
    }
    mapResult.put(sXKey, list0);
    for(int j = 0; j < asYKeys.length; j++) {
      List<Object> listJ = new ArrayList<Object>(listOfList.size());
      for(int i = 0; i < listOfList.size(); i++) {
        List<Object> record = listOfList.get(i);
        listJ.add(record.get(j+1));
      }
      mapResult.put(asYKeys[j], listJ);
    }
    return mapResult;
  }
  
  private static
  List<Object> getXValues(List<List<Object>> listOfList)
  {
    if(listOfList == null || listOfList.size() == 0) {
      return new ArrayList<Object>(0);
    }
    List<Object> listResult = new ArrayList<Object>();
    for(int i = 0; i < listOfList.size(); i++) {
      List<Object> record = listOfList.get(i);
      if(record.size() > 0) listResult.add(record.get(0));
    }
    return listResult;
  }
  
  private static
  List<List<Object>> getYValues(List<List<Object>> listOfList)
  {
    if(listOfList == null || listOfList.size() == 0) {
      return new ArrayList<List<Object>>(0);
    }
    List<Object> record0 = listOfList.get(0);
    int iSizeY   = record0.size() - 1;
    if(iSizeY < 1) return new ArrayList<List<Object>>(0);
    List<List<Object>> listResult = new ArrayList<List<Object>>(iSizeY);
    for(int j = 1; j <= iSizeY; j++) {
      listResult.add(new ArrayList<Object>());
    }
    for(int i = 0; i < listOfList.size(); i++) {
      List<Object> record = listOfList.get(i);
      for(int j = 1; j <= iSizeY; j++) {
        List<Object> listJ = listResult.get(j-1);
        listJ.add(record.get(j));
      }
    }
    return listResult;
  }
  
  private static
  void calcPercentages(List<List<Object>> listOfList)
  {
    if(listOfList == null || listOfList.size() == 0) return;
    List<Object> record0 = listOfList.get(0);
    int iSizeY   = record0.size() - 1;
    if(iSizeY < 1) return;
    List<Object> listTotals = new ArrayList<Object>(iSizeY);
    for(int j = 1; j <= iSizeY; j++) {
      double dTotalJ = 0.0d;
      for(int i = 0; i < listOfList.size(); i++) {
        List<Object> record = listOfList.get(i);
        Object oValue = record.get(j);
        if(oValue instanceof Number) {
          dTotalJ += ((Number) oValue).doubleValue();
        }
      }
      listTotals.add(new Double(dTotalJ));
    }
    for(int j = 1; j <= iSizeY; j++) {
      Number oTotalJ = (Number) listTotals.get(j-1);
      double dTotalJ = oTotalJ.doubleValue();
      if(dTotalJ == 0.0d) continue;
      for(int i = 0; i < listOfList.size(); i++) {
        List<Object> record = listOfList.get(i);
        Object oValue = record.get(j);
        if(oValue instanceof Number) {
          double dValue = ((Number) oValue).doubleValue();
          long   lPerc  = Math.round((dValue*10000.0d)/dTotalJ);
          record.set(j, new Double(lPerc/100.0d));
        }
      }
    }
  }
  
  private static
  double[] calcMinMaxSumAvg(List<List<Object>> listOfList)
  {
    // 0=min,1=max,2=sum,3=avg
    double[] adResult = {0.0d, 0.0d, 0.0d, 0.0d};
    if(listOfList == null || listOfList.size() == 0) return adResult;
    double dMin = Double.MAX_VALUE;
    double dMax = Double.MIN_VALUE;
    double dSum = 0.0d;
    List<Object> record0 = listOfList.get(0);
    int iSizeY   = record0.size() - 1;
    if(iSizeY < 1) return adResult;
    boolean boAtLeastOne = false;
    for(int j = 1; j <= iSizeY; j++) {
      for(int i = 0; i < listOfList.size(); i++) {
        List<Object> record = listOfList.get(i);
        Object oValue = record.get(j);
        if(oValue instanceof Number) {
          double dValue = ((Number) oValue).doubleValue();
          if(dValue < dMin) dMin = dValue;
          if(dValue > dMax) dMax = dValue;
          dSum += dValue;
          boAtLeastOne = true;
        }
      }
    }
    if(boAtLeastOne) {
      adResult[0] = dMin;
      adResult[1] = dMax;
      adResult[2] = dSum;
      adResult[3] = dSum / listOfList.size();
    }
    return adResult;
  }
  
  private static
  int[] findClause(String sSQL, int iPar)
  {
    if(iPar < 1) return null;
    int iBegin = -1;
    int iEnd   = -1;
    StringBuffer sbCheck = new StringBuffer();
    int iTokens = 0;
    char cLast = '&';
    for(int i=iPar-1; i >= 0; i--) {
      char c = sSQL.charAt(i);
      if(c == '\'' && i != iPar-1) return null;
      if(c == ',') {
        iBegin = i;
        break;
      }
      if(c == ')') {
        iBegin = i+1;
        break;
      }
      sbCheck.append(c);
      if(c == ' ') {
        if(cLast != ' ') iTokens++;
        if(iTokens > 5) break;
        if(sbCheck.toString().endsWith(" dna ") || sbCheck.toString().endsWith(" DNA ")) {
          iBegin = i+1;
          break;
        }
        else if(sbCheck.toString().endsWith(" ro ") || sbCheck.toString().endsWith(" RO ")) {
          iBegin = i+1;
          break;
        }
        else if(sbCheck.toString().endsWith(" erehw ") || sbCheck.toString().endsWith(" EREHW ")) {
          return null;
        }
      }
      cLast = c;
    }
    if(iBegin <= 7) return null;
    for(int i=iPar+1; i < sSQL.length(); i++) {
      char c = sSQL.charAt(i);
      if(c == ' ' || c == ')' || c == ',') {
        iEnd = i;
        break;
      }
    }
    return new int[] {iBegin, iEnd};
  }
  
  private static
  Object sum(Object val1, Object val2)
  {
    if(val1 == null && val2 == null) {
      return new Integer(0);
    }
    if(val1 != null && val2 == null) {
      return val1;
    }
    if(val1 == null && val2 != null) {
      return val2;
    }
    if(val1 instanceof Double) {
      return new Double(((Double) val1).doubleValue() + WUtil.toDouble(val2, 0.0d));
    }
    if(val1 instanceof Integer) {
      return new Integer(((Integer) val1).intValue() + WUtil.toInt(val2, 0));
    }
    if(val1 instanceof Long) {
      return new Long(((Long) val1).longValue() + WUtil.toLong(val2, 0l));
    }
    return val1;
  }
  
  public static
  String evalDateExpression(String sValue)
  {
    if(sValue == null) return sValue;
    int iDate = sValue.indexOf("$date");
    if(iDate < 0) return sValue;
    
    String sLeft = sValue.substring(0, iDate);
    
    Calendar cal = Calendar.getInstance();
    
    int length = sValue.length();
    if(iDate == length-5) {
      return (sLeft + WUtil.formatDate(cal, "-")).trim();
    }
    
    int iSign = 1;
    int iOper = -1;
    int iLast = length - 1;
    for(int i = iDate+5; i < length; i++) {
      char c = sValue.charAt(i);
      if(c < 33) continue;
      if(c == '+') {
        iOper = i;
        continue;
      }
      if(c == '-') {
        iOper =  i;
        iSign = -1;
        continue;
      }
      if(Character.isDigit(c)) {
        iLast = i;
        continue;
      }
      if(Character.isLetter(c)) {
        iLast = i;
      }
      break;
    }
    
    String sRigth = sValue.substring(iLast+1);
    if(iOper < 0) {
      return (sLeft + WUtil.formatDate(cal, "-") + sRigth).trim();
    }
    
    char cLast = sValue.charAt(iLast);
    if(cLast == 'd' || cLast == 'D') {
      int iN = WUtil.toInt(sValue.substring(iOper+1, iLast).trim(), 0);
      cal.add(Calendar.DATE, iN * iSign);
    }
    else if(cLast == 'w' || cLast == 'W') {
      int iN = WUtil.toInt(sValue.substring(iOper+1, iLast).trim(), 0);
      cal.add(Calendar.DATE, iN * 7 * iSign);
    }
    else if(cLast == 'm' || cLast == 'M') {
      int iN = WUtil.toInt(sValue.substring(iOper+1, iLast).trim(), 0);
      cal.add(Calendar.MONTH, iN * iSign);
    }
    else if(cLast == 'f' || cLast == 'f') {
      int iN = WUtil.toInt(sValue.substring(iOper+1, iLast).trim(), 0);
      if(iSign < 0) iN = iN - 1;
      if(iN > 0) cal.add(Calendar.MONTH, iN * iSign);			
      cal.set(Calendar.DATE, 1);
    }
    else if(cLast == 'l' || cLast == 'l') {
      int iN = WUtil.toInt(sValue.substring(iOper+1, iLast).trim(), 0);
      if(iSign < 0) iN = iN - 1;
      if(iN > 0) cal.add(Calendar.MONTH, iN * iSign);
      cal.set(Calendar.DATE, 1);
      cal.add(Calendar.DATE, -1);
    }
    else if(cLast == 'y' || cLast == 'Y') {
      int iN = WUtil.toInt(sValue.substring(iOper+1, iLast).trim(), 0);
      cal.add(Calendar.YEAR, iN * iSign);
    }
    else {
      if(Character.isDigit(cLast)) {
        int iN = WUtil.toInt(sValue.substring(iOper+1, iLast+1).trim(), 0);
        cal.add(Calendar.DATE, iN * iSign);
      }
      else {
        int iN = WUtil.toInt(sValue.substring(iOper+1, iLast).trim(), 0);
        cal.add(Calendar.DATE, iN * iSign);
      }
    }
    return (sLeft + WUtil.formatDate(cal, "-") + sRigth).trim();
  }
}
