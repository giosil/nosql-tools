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

import java.util.*;

public
class ListSorter
{
  public static <T>
  List<T> reverse(List<T> list)
  {
    List<T> listResult = new ArrayList<T>(list.size());
    for(int i=list.size()-1; i >= 0; i--) {
      listResult.add(list.get(i));
    }
    return listResult;
  }
  
  @SuppressWarnings("unchecked")
  public static
  void sortListOfList(List<List<Object>> listData, int iIndex)
  {
    if(listData == null || listData.size() < 2) return;
    int iFirst = 0;
    int iLast = listData.size() - 1;
    boolean boSorted = true;
    do {
      for(int i = iLast; i > iFirst; i--) {
        List<Object> l1 = listData.get(i);
        List<Object> l2 = listData.get(i - 1);
        Object o1  = l1.get(iIndex);
        Object o2  = l2.get(iIndex);
        boolean lt = false;
        if(o1 instanceof Comparable && o2 instanceof Comparable) {
          lt = ((Comparable<Object>) o1).compareTo(o2) < 0;
        }
        else {
          lt = o1 == null && o2 != null;
        }
        if(lt) {
          listData.set(i,     l2);
          listData.set(i - 1, l1);
          boSorted = false;
        }
      }
      iFirst++;
    }
    while((iLast > iFirst) &&(!boSorted));
  }
  
  @SuppressWarnings("unchecked")
  public static
  void sortListOfMap(List<Map<String,Object>> listData, Object oKey)
  {
    if(listData == null || listData.size() < 2) return;
    int iFirst = 0;
    int iLast  = listData.size() - 1;
    boolean boSorted = true;
    do {
      for(int i = iLast; i > iFirst; i--) {
        Map<String,Object> m1 = listData.get(i);
        Map<String,Object> m2 = listData.get(i - 1);
        Object o1  = m1.get(oKey);
        Object o2  = m2.get(oKey);
        boolean lt = false;
        if(o1 instanceof Comparable && o2 instanceof Comparable) {
          lt = ((Comparable<Object>) o1).compareTo(o2) < 0;
        }
        else {
          lt = o1 == null && o2 != null;
        }
        if(lt) {
          listData.set(i,   m2);
          listData.set(i-1, m1);
          boSorted = false;
        }
      }
      iFirst++;
    }
    while((iLast > iFirst) &&(!boSorted));
  }
}
