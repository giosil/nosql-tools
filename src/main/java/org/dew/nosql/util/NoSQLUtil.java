package org.dew.nosql.util;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.dew.nosql.INoSQLDB;
import org.dew.nosql.NoSQLDataSource;

public 
class NoSQLUtil 
{
  public static int DEF_NOSQL_MAX_ROWS = 1000;
  
  public static
  List<Map<String,Object>> find(String sCollection, Map<String,Object> mapFilter)
    throws Exception
  {
    return find(sCollection, mapFilter, (List<String>) null, (String) null, 0);
  }
  
  public static
  List<Map<String,Object>> find(String sCollection, Map<String,Object> mapFilter, List<String> listFields)
    throws Exception
  {
    return find(sCollection, mapFilter, listFields, (String) null, 0);
  }
  
  public static
  List<Map<String,Object>> find(String sCollection, Map<String,Object> mapFilter, List<String> listFields, String sOrderBy)
    throws Exception
  {
    return find(sCollection, mapFilter, listFields, sOrderBy, 0);
  }
  
  public static
  List<Map<String,Object>> find(String sCollection, Map<String,Object> mapFilter, List<String> listFields, String sOrderBy, int iMaxRows)
    throws Exception
  {
    if(mapFilter == null) mapFilter = new HashMap<String,Object>();
    
    int iFltMaxRows   = WUtil.toInt(mapFilter.remove("__maxrows__"), 0);
    if(iFltMaxRows > 0) {
      iMaxRows = iFltMaxRows;
    }
    int iPage = WUtil.toInt(mapFilter.remove("__page__"), 0);
    
    boolean ascOrder = false;
    if(iPage < 0) {
      iPage = 0;
      ascOrder = true;
    }
    
    if(sOrderBy == null || sOrderBy.length() == 0) {
      sOrderBy = "_id ";
      if(!ascOrder) {
        sOrderBy += " desc";
      }
    }
    
    Set<String> setEncFields = new HashSet<String>();
    for(int i = 1; i <= 10; i++) {
      Object oEncField = mapFilter.remove("_" + i);
      if(oEncField instanceof String) {
        String sEncField = (String) oEncField;
        if(sEncField.length() > 0) {
          setEncFields.add(sEncField);
          
          Object value = mapFilter.get(sEncField);
          if(value instanceof String) {
            mapFilter.put(sEncField, Obfuscator.encrypt((String) value));
          }
        }
      }
    }
    
    if(iMaxRows < 1) iMaxRows = DEF_NOSQL_MAX_ROWS;
    
    String fields = "";
    if(listFields != null && listFields.size() > 0) {
      for(int i = 0; i < listFields.size(); i++) {
        fields += "," + listFields.get(i);
      }
      fields = fields.substring(1);
    }
    
    List<Map<String,Object>> listResult = new ArrayList<Map<String,Object>>();
    
    long count = -1;
    try {
      INoSQLDB noSQLDB = NoSQLDataSource.getDefaultNoSQLDB();
      
      Map<String, Object> query = buildNoSQLFilter(mapFilter);
      
      if(iPage > 0) {
        count = noSQLDB.count(sCollection, query);
      }
      
      if(iPage > 1 && iMaxRows > 0) {
        int skip = (iPage - 1) * iMaxRows;
        
        listResult = noSQLDB.find(sCollection, query, fields, sOrderBy, iMaxRows, skip);
      }
      else {
        listResult = noSQLDB.find(sCollection, query, fields, sOrderBy, iMaxRows);
      }
      
      for(int i = 0; i < listResult.size(); i++) {
        Map<String, Object> mapRecord = listResult.get(i);
        
        Iterator<String> iteratorEnc = setEncFields.iterator();
        while(iteratorEnc.hasNext()) {
          String field = iteratorEnc.next();
          Object value = mapRecord.get(field);
          if(value instanceof String) {
            mapRecord.put(field, Obfuscator.decrypt((String) value));
          }
          else if(value instanceof Collection) {
            Collection<?> col = (Collection<?>) value;
            List<Object> list = new ArrayList<Object>(col.size());
            Iterator<?> iteratorCol = col.iterator();
            while(iteratorCol.hasNext()) {
              Object item = iteratorCol.next();
              if(item instanceof String) {
                list.add(Obfuscator.decrypt((String) item));
              }
            }
            if(list.size() > 0) mapRecord.put(field, list);
          }
        }
        
      }
    }
    catch(Exception ex) {
      System.out.println("Exception in NoSQLUtil.find(" + sCollection + "," + mapFilter + "," + listFields + "," + sOrderBy + "," + iMaxRows + "): " + ex);
      throw ex;
    }
    
    if(listResult.size() > 0) {
      Map<String, Object> item0 = listResult.get(0);
      if(item0 != null) {
        item0.put("__c", count);
        item0.put("__r", iMaxRows);
        if(iMaxRows > 0 && count > 0) {
          long pages = count / iMaxRows;
          if((count % iMaxRows) > 0) pages++;
          item0.put("__p", pages);
        }
        else {
          item0.put("__p", 1);
        }
      }
    }
    
    return listResult;
  }
  
  public static
  Map<String, Object> buildNoSQLFilter(Map<String, Object> map)
  {
    Map<String, Object> result = new HashMap<String, Object>();
    
    if(map == null || map.isEmpty()) return result;
    
    Object oTime = map.remove("__time__");
    Calendar calFromTime = null;
    Calendar calToTime   = null;
    if(!WUtil.isBlank(oTime)) {
      int iTime = WUtil.toIntTime(oTime, 0);
      int iHH   = iTime / 100;
      int iMM   = iTime % 100;
      
      calFromTime = WUtil.getCurrentDate();
      calFromTime.set(Calendar.HOUR_OF_DAY, iHH);
      calFromTime.set(Calendar.MINUTE,      iMM);
      calFromTime.add(Calendar.MINUTE,      -10);
      
      calToTime = WUtil.getCurrentDate();
      calToTime.set(Calendar.HOUR_OF_DAY, iHH);
      calToTime.set(Calendar.MINUTE,      iMM);
      calToTime.add(Calendar.MINUTE,       10);
    }
    
    Iterator<Map.Entry<String, Object>> iterator = map.entrySet().iterator();
    while(iterator.hasNext()) {
      Map.Entry<String, Object> entry = iterator.next();
      
      String sKey  = entry.getKey();
      Object value = entry.getValue();
      
      if(value instanceof Collection) {
        result.put(sKey, value);
        continue;
      }
      if(value != null && value.getClass().isArray()) {
        result.put(sKey, value);
        continue;
      }
      if(sKey.equals("_id")) {
        if(value instanceof String) {
          result.put(sKey, value);
          continue;
        }
      }
      
      boolean boStartsWithPerc = false;
      boolean boEndsWithPerc   = false;
      boStartsWithPerc = sKey.startsWith("x__");
      if(boStartsWithPerc) sKey = sKey.substring(3);
      boEndsWithPerc = sKey.endsWith("__x");
      if(boEndsWithPerc) sKey = sKey.substring(0, sKey.length() - 3);
      
      boolean boGTE  = sKey.endsWith("__gte");
      boolean boLTE  = sKey.endsWith("__lte");
      boolean boNE   = sKey.endsWith("__neq");
      if(!boNE) boNE = sKey.endsWith("__not");
      if(boGTE || boLTE || boNE) sKey = sKey.substring(0, sKey.length() - 5);
      
      boolean boGT   = sKey.endsWith("__gt");
      boolean boLT   = sKey.endsWith("__lt");
      boolean boNN   = sKey.endsWith("__nn");
      if(boGT || boLT || boNN) sKey = sKey.substring(0, sKey.length() - 4);
      
      if(boNN) {
        boolean boValue = WUtil.toBoolean(value, false);
        if(boValue) {
          result.put(sKey + "!=", null);
        }
        continue;
      }
      
      if(value instanceof Calendar) {
        value = ((Calendar) value).getTime();
        if(boGTE || boGT) {
          if(calFromTime != null) {
            Calendar cal = Calendar.getInstance();
            cal.setTimeInMillis(((Date) value).getTime());
            cal.set(Calendar.HOUR_OF_DAY, calFromTime.get(Calendar.HOUR_OF_DAY));
            cal.set(Calendar.MINUTE,      calFromTime.get(Calendar.MINUTE));
            cal.set(Calendar.SECOND,      0);
            cal.set(Calendar.MILLISECOND, 0);
            value = new java.util.Date(cal.getTimeInMillis());
          }
        }
        else if(boLT) {
          if(calToTime != null) {
            Calendar cal = Calendar.getInstance();
            cal.setTimeInMillis(((Date) value).getTime());
            cal.set(Calendar.HOUR_OF_DAY, calToTime.get(Calendar.HOUR_OF_DAY));
            cal.set(Calendar.MINUTE,      calToTime.get(Calendar.MINUTE));
            cal.set(Calendar.SECOND,      0);
            cal.set(Calendar.MILLISECOND, 0);
            value = new java.util.Date(cal.getTimeInMillis());
          }
        }
        else if(boLTE) {
          Calendar cal = Calendar.getInstance();
          cal.setTimeInMillis(((Date) value).getTime());
          if(calToTime != null) {
            cal.set(Calendar.HOUR_OF_DAY, calToTime.get(Calendar.HOUR_OF_DAY));
            cal.set(Calendar.MINUTE,      calToTime.get(Calendar.MINUTE));
            cal.set(Calendar.SECOND,      0);
            cal.set(Calendar.MILLISECOND, 0);
          }
          else if(cal.get(Calendar.HOUR_OF_DAY) == 0) {
            cal.add(Calendar.DATE, 1);
            boLTE = false;
            boLT  = true;
          }
          value = new java.util.Date(cal.getTimeInMillis());
        }
      }
      
      if(value instanceof Date) {
        if(boGTE || boGT) {
          if(calFromTime != null) {
            Calendar cal = Calendar.getInstance();
            cal.setTimeInMillis(((Date) value).getTime());
            cal.set(Calendar.HOUR_OF_DAY, calFromTime.get(Calendar.HOUR_OF_DAY));
            cal.set(Calendar.MINUTE,      calFromTime.get(Calendar.MINUTE));
            cal.set(Calendar.SECOND,      0);
            cal.set(Calendar.MILLISECOND, 0);
            value = new java.util.Date(cal.getTimeInMillis());
          }
        }
        else if(boLT) {
          if(calToTime != null) {
            Calendar cal = Calendar.getInstance();
            cal.setTimeInMillis(((Date) value).getTime());
            cal.set(Calendar.HOUR_OF_DAY, calToTime.get(Calendar.HOUR_OF_DAY));
            cal.set(Calendar.MINUTE,      calToTime.get(Calendar.MINUTE));
            cal.set(Calendar.SECOND,      0);
            cal.set(Calendar.MILLISECOND, 0);
            value = new java.util.Date(cal.getTimeInMillis());
          }
        }
        else if(boLTE) {
          Calendar cal = Calendar.getInstance();
          cal.setTimeInMillis(((Date) value).getTime());
          if(calToTime != null) {
            cal.set(Calendar.HOUR_OF_DAY, calToTime.get(Calendar.HOUR_OF_DAY));
            cal.set(Calendar.MINUTE,      calToTime.get(Calendar.MINUTE));
            cal.set(Calendar.SECOND,      0);
            cal.set(Calendar.MILLISECOND, 0);
          }
          else if(cal.get(Calendar.HOUR_OF_DAY) == 0) {
            cal.add(Calendar.DATE, 1);
            boLTE = false;
            boLT  = true;
          }
          value = new java.util.Date(cal.getTimeInMillis());
        }
      }
      
      if(value instanceof String) {
        String s = ((String) value).trim();
        // Is a date?
        char c0 = s.charAt(0);
        char cL = s.charAt(s.length()-1);
        if(Character.isDigit(c0) && Character.isDigit(cL) && s.length() > 7 && s.length() < 11) {
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
                if(boGTE || boGT) {
                  if(calFromTime != null) {
                    cal.set(Calendar.HOUR_OF_DAY, calFromTime.get(Calendar.HOUR_OF_DAY));
                    cal.set(Calendar.MINUTE,      calFromTime.get(Calendar.MINUTE));
                    cal.set(Calendar.SECOND,      0);
                    cal.set(Calendar.MILLISECOND, 0);
                  }
                }
                else if(boLT) {
                  if(calToTime != null) {
                    cal.set(Calendar.HOUR_OF_DAY, calToTime.get(Calendar.HOUR_OF_DAY));
                    cal.set(Calendar.MINUTE,      calToTime.get(Calendar.MINUTE));
                    cal.set(Calendar.SECOND,      0);
                    cal.set(Calendar.MILLISECOND, 0);
                  }
                }
                if(boLTE) {
                  if(calToTime != null) {
                    cal.set(Calendar.HOUR_OF_DAY, calToTime.get(Calendar.HOUR_OF_DAY));
                    cal.set(Calendar.MINUTE,      calToTime.get(Calendar.MINUTE));
                    cal.set(Calendar.SECOND,      0);
                    cal.set(Calendar.MILLISECOND, 0);
                  }
                  else if(cal.get(Calendar.HOUR_OF_DAY) == 0) {
                    cal.add(Calendar.DATE, 1);
                    boLTE = false;
                    boLT  = true;
                  }
                }
                value = new java.util.Date(cal.getTimeInMillis());
              }
            }
          }
        }
      }
      
      if(value instanceof String) {
        String sValue = ((String) value).trim();
        if(sValue.length() == 0) continue;
        
        if(sValue.startsWith("%") || sValue.startsWith("*")) {
          sValue = sValue.substring(1);
          boStartsWithPerc = true;
        }
        if(sValue.endsWith("%") || sValue.endsWith("*")) {
          sValue = sValue.substring(0, sValue.length()-1);
          boEndsWithPerc = true;
        }
        if(boStartsWithPerc || boEndsWithPerc) {
          String sRegExp = "";
          if(boStartsWithPerc) sRegExp += "%";
          sRegExp += sValue;
          if(boEndsWithPerc) sRegExp += "%";
          result.put(sKey, sRegExp);
          continue;
        }
        if(sValue.equalsIgnoreCase("null")) {
          value = null;
        }
      }
      
      if(boNE) {
        result.put(sKey + "!=", value);
      }
      else if(boGT) {
        result.put(sKey + ">", value);
      }
      else if(boLT) {
        result.put(sKey + "<", value);
      }
      else if(boGTE) {
        result.put(sKey + ">=", value);
      }
      else if(boLTE) {
        result.put(sKey + "<=", value);
      }
      else {
        result.put(sKey, value);
      }
    }
    return result;
  }
}
