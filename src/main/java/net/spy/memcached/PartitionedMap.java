/*
 * arcus-java-client : Arcus Java client
 * Copyright 2010-2014 NAVER Corp.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.spy.memcached;

import java.util.AbstractList;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class PartitionedMap<K, V> extends AbstractList<Map<K, V>> {

  private final List<Map<K, V>> mapList;

  public PartitionedMap(Map<K, V> map, int size) {
    int expectSize = (map.size() + size - 1) / size;
    int splitBy = size;
    int parsedSize = 0;

    int mapSize = map.size();
    int counter = 0;
    int listIndex = 0;

    mapList = new ArrayList<Map<K, V>>(expectSize);
    for (int i = 0; i < expectSize; i++) {
      mapList.add(new HashMap<K, V>());
    }

    for (Map.Entry<K, V> entry : map.entrySet()) {
      parsedSize++;
      counter++;

      mapList.get(listIndex).put(entry.getKey(), entry.getValue());

      if (parsedSize == splitBy || counter == mapSize) {
        parsedSize = 0;
        listIndex++;
      }
    }
  }

  @Override
  public Map<K, V> get(int index) {
    int listSize = size();

    if (listSize < 0) {
      throw new IllegalArgumentException("negative size: " + listSize);
    }

    if (index < 0) {
      throw new IndexOutOfBoundsException("index " + index
          + " must not be negative");
    }

    if (index >= listSize) {
      throw new IndexOutOfBoundsException("index " + index
          + " must be less than size " + listSize);
    }

    return mapList.get(index);
  }

  @Override
  public int size() {
    return mapList.size();
  }

  @Override
  public boolean isEmpty() {
    return mapList.isEmpty();
  }
}
