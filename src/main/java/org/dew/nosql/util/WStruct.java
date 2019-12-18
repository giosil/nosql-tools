/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.dew.nosql.util;

import java.lang.reflect.Array;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

@SuppressWarnings({"rawtypes","unchecked"})
public
class WStruct extends WMap
{
  public WStruct()
  {
    super();
  }
  
  public WStruct(boolean boLegacy)
  {
    super(boLegacy);
  }
  
  public WStruct(int initialCapacity)
  {
    super(initialCapacity);
  }
  
  public WStruct(int initialCapacity, float loadFactor)
  {
    super(initialCapacity, loadFactor);
  }
  
  public WStruct(Map m)
  {
    super(m);
  }
  
  public Object get(Object key) {
    if(key instanceof String) {
      return get(map,(String) key);
    }
    return super.get(key);
  }
  
  public Object put(Object key, Object value) {
    if(key instanceof String) {
      String sKey  = (String) key;
      int iIndexOf = sKey.indexOf('.');
      if(iIndexOf < 0) {
        return super.put(key, value);
      }
      ArrayList keys = new ArrayList();
      int iBegin   = 0;
      while(iIndexOf >= 0) {
        keys.add(sKey.substring(iBegin, iIndexOf));
        iBegin = iIndexOf + 1;
        iIndexOf = sKey.indexOf('.', iBegin);
      }
      String lastKey = sKey.substring(iBegin);
      Map currentMap = map;
      for(int i = 0; i < keys.size(); i++) {
        String currentKey = (String) keys.get(i);
        Object currentVal = currentMap.get(currentKey);
        if(currentVal instanceof Map) {
          currentMap = (Map) currentVal;
        }
        else {
          Map newMap = null;
          if(map instanceof Hashtable) {
            newMap = new Hashtable();
          }
          else {
            newMap = new HashMap();
          }
          currentMap.put(currentKey, newMap);
          currentMap = newMap;
        }
      }
      if(currentMap instanceof Hashtable && value == null) {
        currentMap.remove(lastKey);
        return null;
      }
      return currentMap.put(lastKey, value);
    }
    return super.put(key, value);
  }
  
  public Object remove(Object key) {
    if(key instanceof String) {
      String sKey  = (String) key;
      int iIndexOf = sKey.indexOf('.');
      if(iIndexOf < 0) {
        return super.remove(key);
      }
      ArrayList keys = new ArrayList();
      int iBegin   = 0;
      while(iIndexOf >= 0) {
        keys.add(sKey.substring(iBegin, iIndexOf));
        iBegin = iIndexOf + 1;
        iIndexOf = sKey.indexOf('.', iBegin);
      }
      String lastKey = sKey.substring(iBegin);
      Map currentMap = map;
      for(int i = 0; i < keys.size(); i++) {
        String currentKey = (String) keys.get(i);
        Object currentVal = currentMap.get(currentKey);
        if(currentVal instanceof Map) {
          currentMap = (Map) currentVal;
        }
        else {
          return null;
        }
      }
      return currentMap.remove(lastKey);
    }
    return map.remove(key);
  }
  
  public void putAll(Map m) {
    if(m == null) return;
    Iterator iterator = m.entrySet().iterator();
    while(iterator.hasNext()) {
      Map.Entry entry = (Map.Entry) iterator.next();
      put(entry.getKey(), entry.getValue());
    }
  }
  
  protected static Object get(Map src, String keysrc) {
    Integer idxsrc = null;
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
      int iIdx = keysrc.indexOf('[');
      if(iIdx > 0) { // array index
        int iEndIdx = keysrc.indexOf(']',iIdx+1);
        if(iEndIdx > 0) {
          String sIdxsrc = keysrc.substring(iIdx+1, iEndIdx);
          try{ idxsrc = new Integer(sIdxsrc); } catch(Exception ex) {}
        }
        keysrc = keysrc.substring(0, iIdx);
      }
      valsrc = src.get(keysrc);
    }
    if(valsrc instanceof List && idxsrc != null) {
      List listValSrc = (List) valsrc;
      int idx = idxsrc.intValue();
      if(idx < 0) {
        idx = listValSrc.size() + idx;
        if(idx < 0) valsrc = null;
      }
      if(idx >= 0 && listValSrc.size() > idx) {
        valsrc = listValSrc.get(idx);
      }
      else {
        valsrc = null;
      }
    }
    else
    if(valsrc != null && valsrc.getClass().isArray() && idxsrc != null) {
      int length = Array.getLength(valsrc);
      int idx = idxsrc.intValue();
      if(idx < 0) {
        idx = length + idx;
        if(idx < 0) valsrc = null;
      }
      if(idx >= 0 && length > idx) {
        valsrc = Array.get(valsrc, idx);
      }
      else {
        valsrc = null;
      }
    }
    return valsrc;
  }
}
