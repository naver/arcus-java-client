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
package net.spy.memcached.collection.btree;

import java.util.Arrays;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;

import net.spy.memcached.collection.BTreeOrder;
import net.spy.memcached.collection.BaseIntegrationTest;
import net.spy.memcached.collection.CollectionAttributes;
import net.spy.memcached.collection.CollectionResponse;
import net.spy.memcached.collection.Element;
import net.spy.memcached.internal.CollectionFuture;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class BopFindPositionWithGetTest extends BaseIntegrationTest {

  private String key = "BopFindPositionWithGetTest";
  private String invalidKey = "InvalidBopFindPositionWithGetTest";
  private String kvKey = "KvBopFindPositionWithGetTest";

  @BeforeEach
  @Override
  protected void setUp() throws Exception {
    super.setUp();
    mc.delete(key).get(1000, TimeUnit.MILLISECONDS);
  }

  @Test
  public void testLongBKeyAsc() throws Exception {
    long longBkey, resultBkey;
    int totCount = 100;
    int pwgCount = 10;
    int rstCount;
    int position, i;

    CollectionAttributes attrs = new CollectionAttributes();
    for (i = 0; i < totCount; i++) {
      longBkey = (long) i;
      mc.asyncBopInsert(key, longBkey, null, "val", attrs).get();
    }

    for (i = 0; i < totCount; i++) {
      longBkey = (long) i;
      CollectionFuture<Map<Integer, Element<Object>>> f = mc
              .asyncBopFindPositionWithGet(key, longBkey, BTreeOrder.ASC, pwgCount);
      Map<Integer, Element<Object>> result = f.get(1000, TimeUnit.MILLISECONDS);

      if (i >= pwgCount && i < (totCount - pwgCount)) {
        rstCount = pwgCount + 1 + pwgCount;
      } else {
        if (i < pwgCount) {
          rstCount = i + 1 + pwgCount;
        } else {
          rstCount = pwgCount + 1 + ((totCount - 1) - i);
        }
      }
      assertEquals(rstCount, result.size());
      assertEquals(CollectionResponse.END, f.getOperationStatus().getResponse());

      if (i < pwgCount) {
        position = 0;
      } else {
        position = i - pwgCount;
      }
      resultBkey = position;
      for (Entry<Integer, Element<Object>> each : result.entrySet()) {
        assertEquals(position, each.getKey().intValue(), "invalid position");
        assertEquals(resultBkey, each.getValue().getLongBkey(), "invalid bkey");
        assertEquals("val", each.getValue().getValue(), "invalid value");
        position++;
        resultBkey++;
      }
    }
  }

  @Test
  public void testLongBKeyDesc() throws Exception {
    long longBkey, resultBkey;
    int totCount = 100;
    int pwgCount = 10;
    int rstCount;
    int position, i;

    CollectionAttributes attrs = new CollectionAttributes();
    for (i = 0; i < totCount; i++) {
      longBkey = (long) i;
      mc.asyncBopInsert(key, longBkey, null, "val", attrs).get();
    }

    for (i = 0; i < totCount; i++) {
      longBkey = (long) i;
      CollectionFuture<Map<Integer, Element<Object>>> f = mc
              .asyncBopFindPositionWithGet(key, longBkey, BTreeOrder.DESC, pwgCount);
      Map<Integer, Element<Object>> result = f.get(1000, TimeUnit.MILLISECONDS);

      if (i >= pwgCount && i < (totCount - pwgCount)) {
        rstCount = pwgCount + 1 + pwgCount;
      } else {
        if (i < pwgCount) {
          rstCount = pwgCount + 1 + i;
        } else {
          rstCount = ((totCount - 1) - i) + 1 + pwgCount;
        }
      }
      assertEquals(rstCount, result.size());
      assertEquals(CollectionResponse.END, f.getOperationStatus().getResponse());

      if (i > ((totCount - 1) - pwgCount)) {
        position = 0;
      } else {
        position = ((totCount - 1) - pwgCount - i);
      }
      resultBkey = (totCount - 1) - position;
      for (Entry<Integer, Element<Object>> each : result.entrySet()) {
        assertEquals(position, each.getKey().intValue(), "invalid position");
        assertEquals(resultBkey, each.getValue().getLongBkey(), "invalid bkey");
        assertEquals("val", each.getValue().getValue(), "invalid value");
        position++;
        resultBkey--;
      }
    }
  }

  @Test
  public void testByteArrayBKeyAsc() throws Exception {
    byte[] byteBkey, resultBkey;
    int totCount = 100;
    int pwgCount = 10;
    int rstCount;
    int position, i, bkey;

    byteBkey = new byte[1];
    resultBkey = new byte[1];

    CollectionAttributes attrs = new CollectionAttributes();
    for (i = 0; i < totCount; i++) {
      byteBkey[0] = (byte) i;
      mc.asyncBopInsert(key, byteBkey, null, "val", attrs).get();
    }

    for (i = 0; i < totCount; i++) {
      byteBkey[0] = (byte) i;
      CollectionFuture<Map<Integer, Element<Object>>> f = mc
              .asyncBopFindPositionWithGet(key, byteBkey, BTreeOrder.ASC, pwgCount);
      Map<Integer, Element<Object>> result = f.get(1000, TimeUnit.MILLISECONDS);

      if (i >= pwgCount && i < (totCount - pwgCount)) {
        rstCount = pwgCount + 1 + pwgCount;
      } else {
        if (i < pwgCount) {
          rstCount = i + 1 + pwgCount;
        } else {
          rstCount = pwgCount + 1 + ((totCount - 1) - i);
        }
      }
      assertEquals(rstCount, result.size());
      assertEquals(CollectionResponse.END, f.getOperationStatus().getResponse());

      if (i < pwgCount) {
        position = 0;
      } else {
        position = i - pwgCount;
      }
      bkey = position;
      resultBkey[0] = (byte) bkey;
      for (Entry<Integer, Element<Object>> each : result.entrySet()) {
        assertEquals(position, each.getKey().intValue(), "invalid position");
        assertTrue(Arrays.equals(resultBkey, each.getValue().getByteArrayBkey()), "invalid bkey");
        assertEquals("val", each.getValue().getValue(), "invalid value");
        position++;
        bkey++;
        resultBkey[0] = (byte) bkey;
      }
    }
  }

  @Test
  public void testByteArrayBKeyDesc() throws Exception {
    byte[] byteBkey, resultBkey;
    int totCount = 100;
    int pwgCount = 10;
    int rstCount;
    int position, i, bkey;

    byteBkey = new byte[1];
    resultBkey = new byte[1];

    CollectionAttributes attrs = new CollectionAttributes();
    for (i = 0; i < totCount; i++) {
      byteBkey[0] = (byte) i;
      mc.asyncBopInsert(key, byteBkey, null, "val", attrs).get();
    }

    for (i = 0; i < totCount; i++) {
      byteBkey[0] = (byte) i;
      CollectionFuture<Map<Integer, Element<Object>>> f = mc
              .asyncBopFindPositionWithGet(key, byteBkey, BTreeOrder.DESC, pwgCount);
      Map<Integer, Element<Object>> result = f.get(1000, TimeUnit.MILLISECONDS);

      if (i >= pwgCount && i < (totCount - pwgCount)) {
        rstCount = pwgCount + 1 + pwgCount;
      } else {
        if (i < pwgCount) {
          rstCount = pwgCount + 1 + i;
        } else {
          rstCount = ((totCount - 1) - i) + 1 + pwgCount;
        }
      }
      assertEquals(rstCount, result.size());
      assertEquals(CollectionResponse.END, f.getOperationStatus().getResponse());

      if (i > ((totCount - 1) - pwgCount)) {
        position = 0;
      } else {
        position = ((totCount - 1) - pwgCount - i);
      }
      bkey = (totCount - 1) - position;
      resultBkey[0] = (byte) bkey;
      for (Entry<Integer, Element<Object>> each : result.entrySet()) {
        assertEquals(position, each.getKey().intValue(), "invalid position");
        assertTrue(Arrays.equals(resultBkey, each.getValue().getByteArrayBkey()), "invalid bkey");
        assertEquals("val", each.getValue().getValue(), "invalid value");
        position++;
        bkey--;
        resultBkey[0] = (byte) bkey;
      }
    }
  }

  @Test
  public void testUnsuccessfulResponses() throws Exception {
    mc.delete(invalidKey).get();
    mc.delete(kvKey).get();

    // insert
    CollectionAttributes attrs = new CollectionAttributes();
    attrs.setReadable(false);
    for (long i = 0; i < 100; i++) {
      mc.asyncBopInsert(key, i, null, "val", attrs).get();
    }

    // set a test key
    mc.set(kvKey, 0, "value").get();

    CollectionFuture<Map<Integer, Element<Object>>> f = null;
    Map<Integer, Element<Object>> result = null;

    // NOT_FOUND
    long longBkey = 10;
    f = mc.asyncBopFindPositionWithGet(invalidKey, longBkey, BTreeOrder.ASC, 0);
    result = f.get();
    assertNull(result);
    assertEquals(CollectionResponse.NOT_FOUND, f.getOperationStatus().getResponse());

    // UNREADABLE
    f = mc.asyncBopFindPositionWithGet(key, longBkey, BTreeOrder.ASC, 0);
    result = f.get();
    assertNull(result);
    assertEquals(CollectionResponse.UNREADABLE, f.getOperationStatus().getResponse());

    attrs.setReadable(true);
    mc.asyncSetAttr(key, attrs).get();

    // TYPE_MISMATCH
    f = mc.asyncBopFindPositionWithGet(kvKey, longBkey, BTreeOrder.ASC, 0);
    result = f.get();
    assertNull(result);
    assertEquals(CollectionResponse.TYPE_MISMATCH, f.getOperationStatus().getResponse());

    // NOT_FOUND_ELEMENT
    longBkey = 200;
    f = mc.asyncBopFindPositionWithGet(key, longBkey, BTreeOrder.ASC, 0);
    result = f.get();
    assertNull(result);
    assertEquals(CollectionResponse.NOT_FOUND_ELEMENT, f.getOperationStatus().getResponse());

    // BKEY_MISMATCH
    byte[] byteBkey = new byte[1];
    byteBkey[0] = (byte) 1;
    f = mc.asyncBopFindPositionWithGet(kvKey, byteBkey, BTreeOrder.ASC, 0);
    result = f.get();
    assertNull(result);
    assertEquals(CollectionResponse.TYPE_MISMATCH, f.getOperationStatus().getResponse());
  }
}
