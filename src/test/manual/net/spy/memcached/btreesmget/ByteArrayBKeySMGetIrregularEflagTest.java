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
package net.spy.memcached.btreesmget;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import net.spy.memcached.collection.BaseIntegrationTest;
import net.spy.memcached.collection.CollectionAttributes;
import net.spy.memcached.collection.ElementFlagFilter;
import net.spy.memcached.collection.SMGetElement;
import net.spy.memcached.collection.SMGetMode;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.fail;

public class ByteArrayBKeySMGetIrregularEflagTest extends BaseIntegrationTest {

  private final String key1 = "ByteArrayBKeySMGetIrregularEflagTest1"
          + (Math.abs(new Random().nextInt(99)) + 100);
  private final String key2 = "ByteArrayBKeySMGetIrregularEflagTest2"
          + (Math.abs(new Random().nextInt(99)) + 100);

  private final byte[] eFlag = {1};

  private final Object value = "valvalvalvalvalvalvalvalvalval";

  @Test
  public void testGetAll_1() {
    ArrayList<String> testKeyList = new ArrayList<>();
    testKeyList.add(key1);
    testKeyList.add(key2);

    /* old SMGetIrregularEflagTest */
    try {
      mc.delete(key1).get();
      mc.delete(key2).get();

      mc.asyncBopInsert(key1, new byte[]{0}, eFlag, value + "0",
              new CollectionAttributes()).get();
      mc.asyncBopInsert(key1, new byte[]{3}, eFlag, value + "1",
              new CollectionAttributes()).get();
      mc.asyncBopInsert(key1, new byte[]{2}, eFlag, value + "2",
              new CollectionAttributes()).get();

      mc.asyncBopInsert(key2, new byte[]{1}, eFlag, value + "0",
              new CollectionAttributes()).get();
      mc.asyncBopInsert(key2, new byte[]{5}, null, value + "1",
              new CollectionAttributes()).get();
      mc.asyncBopInsert(key2, new byte[]{4}, eFlag, value + "2",
              new CollectionAttributes()).get();

      List<SMGetElement<Object>> list = mc.asyncBopSortMergeGet(
              testKeyList, new byte[]{0}, new byte[]{10},
              ElementFlagFilter.DO_NOT_FILTER, 0, 10).get();

      for (int i = 0; i < list.size(); i++) {
        System.out.println(list.get(i));
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }

    try {
      mc.delete(key1).get();
      mc.delete(key2).get();

      mc.asyncBopInsert(key1, new byte[]{0}, eFlag, value + "0",
              new CollectionAttributes()).get();
      mc.asyncBopInsert(key1, new byte[]{3}, eFlag, value + "1",
              new CollectionAttributes()).get();
      mc.asyncBopInsert(key1, new byte[]{2}, eFlag, value + "2",
              new CollectionAttributes()).get();

      mc.asyncBopInsert(key2, new byte[]{1}, eFlag, value + "0",
              new CollectionAttributes()).get();
      mc.asyncBopInsert(key2, new byte[]{5}, null, value + "1",
              new CollectionAttributes()).get();
      mc.asyncBopInsert(key2, new byte[]{4}, eFlag, value + "2",
              new CollectionAttributes()).get();

      SMGetMode smgetMode = SMGetMode.UNIQUE;
      List<SMGetElement<Object>> list = mc.asyncBopSortMergeGet(
              testKeyList, new byte[]{0}, new byte[]{10},
              ElementFlagFilter.DO_NOT_FILTER, 10, smgetMode).get();

      for (int i = 0; i < list.size(); i++) {
        System.out.println(list.get(i));
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }
}
