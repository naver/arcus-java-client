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

import net.spy.memcached.collection.BTreeOrder;
import net.spy.memcached.collection.BaseIntegrationTest;
import net.spy.memcached.collection.CollectionAttributes;
import net.spy.memcached.collection.CollectionOverflowAction;
import net.spy.memcached.collection.CollectionResponse;
import net.spy.memcached.collection.Element;
import net.spy.memcached.internal.BTreeStoreAndGetFuture;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class BopInsertAndGetTest extends BaseIntegrationTest {

  private String key = "BopStoreAndGetTest";
  private String invalidKey = "InvalidBopStoreAndGetTest";
  private String kvKey = "KvBopStoreAndGetTest";

  private long[] longBkeys = {10L, 11L, 12L, 13L, 14L, 15L, 16L, 17L, 18L, 19L};
  private byte[][] byteArrayBkeys = {
      new byte[]{10}, new byte[]{11},
      new byte[]{12}, new byte[]{13}, new byte[]{14},
      new byte[]{15}, new byte[]{16}, new byte[]{17},
      new byte[]{18}, new byte[]{19}
  };

  @BeforeEach
  @Override
  protected void setUp() throws Exception {
    super.setUp();
    mc.delete(key).get();
    mc.delete(invalidKey).get();
    mc.delete(kvKey).get();
  }

  @Test
  public void testInsertAndGetNotTrimmed() throws Exception {
    //given
    CollectionAttributes attrs = new CollectionAttributes();
    attrs.setMaxCount(longBkeys.length + 1);

    //when
    for (long each : longBkeys) {
      mc.asyncBopInsert(key, each, null, "val", attrs).get();
    }

    //then
    BTreeStoreAndGetFuture<Boolean, Object> f = mc
            .asyncBopInsertAndGetTrimmed(key, 2000, null, "val", null);
    assertTrue(f.get());
    assertNull(f.getElement());
  }

  @Test
  public void testInsertAndGetTrimmedLongBKey() throws Exception {
    // insert test data
    CollectionAttributes attrs = new CollectionAttributes();
    attrs.setMaxCount(10);
    attrs.setOverflowAction(CollectionOverflowAction.smallest_trim);
    for (long each : longBkeys) {
      mc.asyncBopInsert(key, each, null, "val", attrs).get();
    }

    // cause an overflow
    assertTrue(mc.asyncBopInsert(key, 1000, null, "val", null).get());

    // expecting that bkey 10 was trimmed out and the first bkey is 11
    Map<Integer, Element<Object>> posMap = mc.asyncBopGetByPosition(key,
            BTreeOrder.ASC, 0).get();
    assertNotNull(posMap);
    assertNotNull(posMap.get(0)); // the first element
    assertEquals(11L, posMap.get(0).getLongBkey());

    // then cause an overflow again and get a trimmed object
    // it would be a bkey(11)
    BTreeStoreAndGetFuture<Boolean, Object> f = mc
            .asyncBopInsertAndGetTrimmed(key, 2000, null, "val", null);
    boolean succeeded = f.get();
    Element<Object> element = f.getElement();
    assertTrue(succeeded);
    assertNotNull(element);
    assertEquals(11L, element.getLongBkey());
    System.out.println("The insertion was succeeded and an element "
            + f.getElement() + " was trimmed out");

    // finally check the first bkey which is expected to be 12
    posMap = mc.asyncBopGetByPosition(key, BTreeOrder.ASC, 0).get();
    assertNotNull(posMap);
    assertNotNull(posMap.get(0)); // the first element
    assertEquals(12L, posMap.get(0).getLongBkey());
  }

  @Test
  public void testInsertAndGetTrimmedByteArrayBKey() throws Exception {
    // insert test data
    CollectionAttributes attrs = new CollectionAttributes();
    attrs.setMaxCount(10);
    attrs.setOverflowAction(CollectionOverflowAction.smallest_trim);
    for (byte[] each : byteArrayBkeys) {
      mc.asyncBopInsert(key, each, null, "val", attrs).get();
    }

    // cause an overflow
    assertTrue(mc.asyncBopInsert(key, new byte[]{64}, null, "val", null)
            .get());

    // expecting that bkey byte(10) was trimmed out and the first bkey is
    // byte(11)
    Map<Integer, Element<Object>> posMap = mc.asyncBopGetByPosition(key,
            BTreeOrder.ASC, 0).get();
    assertNotNull(posMap);
    assertNotNull(posMap.get(0)); // the first element
    assertTrue(Arrays.equals(new byte[]{11}, posMap.get(0)
            .getByteArrayBkey()));

    // then cause an overflow again and get a trimmed object
    // it would be a bkey(11)
    BTreeStoreAndGetFuture<Boolean, Object> f = mc
            .asyncBopInsertAndGetTrimmed(key, new byte[]{65}, null,
                    "val", null);
    boolean succeeded = f.get();
    Element<Object> element = f.getElement();
    assertTrue(succeeded);
    assertNotNull(element);
    assertTrue(Arrays.equals(new byte[]{11}, element.getByteArrayBkey()));
    System.out.println("The insertion was succeeded and an element "
            + f.getElement() + " was trimmed out");

    // finally check the first bkey which is expected to be byte(12)
    posMap = mc.asyncBopGetByPosition(key, BTreeOrder.ASC, 0).get();
    assertNotNull(posMap);
    assertNotNull(posMap.get(0)); // the first element
    assertTrue(Arrays.equals(new byte[]{12}, posMap.get(0)
            .getByteArrayBkey()));
  }

  @Test
  public void testInsertAndGetTrimmedLongBKeyLargest() throws Exception {
    // insert test data
    CollectionAttributes attrs = new CollectionAttributes();
    attrs.setMaxCount(10);
    attrs.setOverflowAction(CollectionOverflowAction.largest_trim);
    for (long each : longBkeys) {
      mc.asyncBopInsert(key, each, null, "val", attrs).get();
    }

    // cause an overflow
    assertTrue(mc.asyncBopInsert(key, 9, null, "val", null).get());

    // expecting that bkey 19 was trimmed out and the last bkey is 18
    Map<Integer, Element<Object>> posMap = mc.asyncBopGetByPosition(key,
            BTreeOrder.DESC, 0).get();
    assertNotNull(posMap);
    assertNotNull(posMap.get(0)); // the first element
    assertEquals(18L, posMap.get(0).getLongBkey());

    // then cause an overflow again and get a trimmed object
    // it would be a bkey(18)
    BTreeStoreAndGetFuture<Boolean, Object> f = mc
            .asyncBopInsertAndGetTrimmed(key, 8, null, "val", null);
    boolean succeeded = f.get();
    Element<Object> element = f.getElement();
    assertTrue(succeeded);
    assertNotNull(element);
    assertEquals(18L, element.getLongBkey());
    System.out.println("The insertion was succeeded and an element "
            + f.getElement() + " was trimmed out");

    // finally check the last bkey which is expected to be 17
    posMap = mc.asyncBopGetByPosition(key, BTreeOrder.DESC, 0).get();
    assertNotNull(posMap);
    assertNotNull(posMap.get(0)); // the first element
    assertEquals(17L, posMap.get(0).getLongBkey());
  }

  @Test
  public void testInsertAndGetTrimmedByteArrayBKeyLargest() throws Exception {
    // insert test data
    CollectionAttributes attrs = new CollectionAttributes();
    attrs.setMaxCount(10);
    attrs.setOverflowAction(CollectionOverflowAction.largest_trim);
    for (byte[] each : byteArrayBkeys) {
      mc.asyncBopInsert(key, each, null, "val", attrs).get();
    }

    // cause an overflow
    assertTrue(mc.asyncBopInsert(key, new byte[]{9}, null, "val", null)
            .get());

    // expecting that bkey byte(19) was trimmed out and the last bkey is
    // byte(18)
    Map<Integer, Element<Object>> posMap = mc.asyncBopGetByPosition(key,
            BTreeOrder.DESC, 0).get();
    assertNotNull(posMap);
    assertNotNull(posMap.get(0)); // the first element
    assertTrue(Arrays.equals(new byte[]{18}, posMap.get(0)
            .getByteArrayBkey()));

    // then cause an overflow again and get a trimmed object
    // it would be a bkey(18)
    BTreeStoreAndGetFuture<Boolean, Object> f = mc
            .asyncBopInsertAndGetTrimmed(key, new byte[]{8}, null,
                    "val", null);
    boolean succeeded = f.get();
    Element<Object> element = f.getElement();
    assertTrue(succeeded);
    assertNotNull(element);
    assertTrue(Arrays.equals(new byte[]{18}, element.getByteArrayBkey()));
    System.out.println("The insertion was succeeded and an element "
            + f.getElement() + " was trimmed out");

    // finally check the last bkey which is expected to be byte(17)
    posMap = mc.asyncBopGetByPosition(key, BTreeOrder.DESC, 0).get();
    assertNotNull(posMap);
    assertNotNull(posMap.get(0)); // the first element
    assertTrue(Arrays.equals(new byte[]{17}, posMap.get(0)
            .getByteArrayBkey()));
  }

  @Test
  public void testUpsertAndGetTrimmedLongBKey() throws Exception {
    // insert test data
    CollectionAttributes attrs = new CollectionAttributes();
    attrs.setMaxCount(10);
    attrs.setOverflowAction(CollectionOverflowAction.smallest_trim);
    for (long each : longBkeys) {
      mc.asyncBopInsert(key, each, null, "val", attrs).get();
    }

    // cause an overflow
    assertTrue(mc.asyncBopInsert(key, 1000, null, "val", null).get());

    // expecting that bkey 10 was trimmed out and the first bkey is 11
    Map<Integer, Element<Object>> posMap = mc.asyncBopGetByPosition(key,
            BTreeOrder.ASC, 0).get();
    assertNotNull(posMap);
    assertNotNull(posMap.get(0)); // the first element
    assertEquals(11L, posMap.get(0).getLongBkey());

    // then cause an overflow again and get a trimmed object
    // it would be a bkey(11)
    BTreeStoreAndGetFuture<Boolean, Object> f = mc
            .asyncBopUpsertAndGetTrimmed(key, 2000, null, "val", null);
    boolean succeeded = f.get();
    Element<Object> element = f.getElement();
    assertTrue(succeeded);
    assertNotNull(element);
    assertEquals(11L, element.getLongBkey());
    System.out.println("The insertion was succeeded and an element "
            + f.getElement() + " was trimmed out");

    // finally check the first bkey which is expected to be 12
    posMap = mc.asyncBopGetByPosition(key, BTreeOrder.ASC, 0).get();
    assertNotNull(posMap);
    assertNotNull(posMap.get(0)); // the first element
    assertEquals(12L, posMap.get(0).getLongBkey());
  }

  @Test
  public void testUpsertAndGetTrimmedByteArrayBKey() throws Exception {
    // insert test data
    CollectionAttributes attrs = new CollectionAttributes();
    attrs.setMaxCount(10);
    attrs.setOverflowAction(CollectionOverflowAction.smallest_trim);
    for (byte[] each : byteArrayBkeys) {
      mc.asyncBopInsert(key, each, null, "val", attrs).get();
    }

    // cause an overflow
    assertTrue(mc.asyncBopInsert(key, new byte[]{64}, null, "val", null)
            .get());

    // expecting that bkey byte(10) was trimmed out and the first bkey is
    // byte(11)
    Map<Integer, Element<Object>> posMap = mc.asyncBopGetByPosition(key,
            BTreeOrder.ASC, 0).get();
    assertNotNull(posMap);
    assertNotNull(posMap.get(0)); // the first element
    assertTrue(Arrays.equals(new byte[]{11}, posMap.get(0)
            .getByteArrayBkey()));

    // then cause an overflow again and get a trimmed object
    // it would be a bkey(11)
    BTreeStoreAndGetFuture<Boolean, Object> f = mc
            .asyncBopUpsertAndGetTrimmed(key, new byte[]{65}, null,
                    "val", null);
    boolean succeeded = f.get();
    Element<Object> element = f.getElement();
    assertTrue(succeeded);
    assertNotNull(element);
    assertTrue(Arrays.equals(new byte[]{11}, element.getByteArrayBkey()));
    System.out.println("The insertion was succeeded and an element "
            + f.getElement() + " was trimmed out");

    // finally check the first bkey which is expected to be byte(12)
    posMap = mc.asyncBopGetByPosition(key, BTreeOrder.ASC, 0).get();
    assertNotNull(posMap);
    assertNotNull(posMap.get(0)); // the first element
    assertTrue(Arrays.equals(new byte[]{12}, posMap.get(0)
            .getByteArrayBkey()));
  }

  @Test
  public void testUpsertAndGetTrimmedLongBKeyLargest() throws Exception {
    // insert test data
    CollectionAttributes attrs = new CollectionAttributes();
    attrs.setMaxCount(10);
    attrs.setOverflowAction(CollectionOverflowAction.largest_trim);
    for (long each : longBkeys) {
      mc.asyncBopInsert(key, each, null, "val", attrs).get();
    }

    // cause an overflow
    assertTrue(mc.asyncBopInsert(key, 9, null, "val", null).get());

    // expecting that bkey 19 was trimmed out and the last bkey is 18
    Map<Integer, Element<Object>> posMap = mc.asyncBopGetByPosition(key,
            BTreeOrder.DESC, 0).get();
    assertNotNull(posMap);
    assertNotNull(posMap.get(0)); // the first element
    assertEquals(18L, posMap.get(0).getLongBkey());

    // then cause an overflow again and get a trimmed object
    // it would be a bkey(18)
    BTreeStoreAndGetFuture<Boolean, Object> f = mc
            .asyncBopUpsertAndGetTrimmed(key, 8, null, "val", null);
    boolean succeeded = f.get();
    Element<Object> element = f.getElement();
    assertTrue(succeeded);
    assertNotNull(element);
    assertEquals(18L, element.getLongBkey());
    System.out.println("The insertion was succeeded and an element "
            + f.getElement() + " was trimmed out");

    // finally check the last bkey which is expected to be 17
    posMap = mc.asyncBopGetByPosition(key, BTreeOrder.DESC, 0).get();
    assertNotNull(posMap);
    assertNotNull(posMap.get(0)); // the first element
    assertEquals(17L, posMap.get(0).getLongBkey());
  }

  @Test
  public void testUpsertAndGetTrimmedByteArrayBKeyLargest() throws Exception {
    // insert test data
    CollectionAttributes attrs = new CollectionAttributes();
    attrs.setMaxCount(10);
    attrs.setOverflowAction(CollectionOverflowAction.largest_trim);
    for (byte[] each : byteArrayBkeys) {
      mc.asyncBopInsert(key, each, null, "val", attrs).get();
    }

    // cause an overflow
    assertTrue(mc.asyncBopInsert(key, new byte[]{9}, null, "val", null)
            .get());

    // expecting that bkey byte(19) was trimmed out and the last bkey is
    // byte(18)
    Map<Integer, Element<Object>> posMap = mc.asyncBopGetByPosition(key,
            BTreeOrder.DESC, 0).get();
    assertNotNull(posMap);
    assertNotNull(posMap.get(0)); // the first element
    assertTrue(Arrays.equals(new byte[]{18}, posMap.get(0)
            .getByteArrayBkey()));

    // then cause an overflow again and get a trimmed object
    // it would be a bkey(18)
    BTreeStoreAndGetFuture<Boolean, Object> f = mc
            .asyncBopUpsertAndGetTrimmed(key, new byte[]{8}, null,
                    "val", null);
    boolean succeeded = f.get();
    Element<Object> element = f.getElement();
    assertTrue(succeeded);
    assertNotNull(element);
    assertTrue(Arrays.equals(new byte[]{18}, element.getByteArrayBkey()));
    System.out.println("The insertion was succeeded and an element "
            + f.getElement() + " was trimmed out");

    // finally check the last bkey which is expected to be byte(17)
    posMap = mc.asyncBopGetByPosition(key, BTreeOrder.DESC, 0).get();
    assertNotNull(posMap);
    assertNotNull(posMap.get(0)); // the first element
    assertTrue(Arrays.equals(new byte[]{17}, posMap.get(0)
            .getByteArrayBkey()));
  }

  @Test
  public void testInsertAndGetTrimmedOtherResponses() throws Exception {
    // insert test data
    CollectionAttributes attrs = new CollectionAttributes();
    attrs.setMaxCount(10);
    attrs.setOverflowAction(CollectionOverflowAction.smallest_trim);
    for (long each : longBkeys) {
      mc.asyncBopInsert(key, each, null, "val", attrs).get();
    }

    // set a test key
    mc.set(kvKey, 0, "value").get();

    BTreeStoreAndGetFuture<Boolean, Object> f = null;
    Boolean result = null;

    // NOT_FOUND
    f = mc.asyncBopInsertAndGetTrimmed(invalidKey, 1, null, "val", null);
    result = f.get();
    assertFalse(result);
    assertEquals(CollectionResponse.NOT_FOUND, f.getOperationStatus()
            .getResponse());

    // OUT_OF_RANGE
    f = mc.asyncBopInsertAndGetTrimmed(key, 1, null, "val", null);
    result = f.get();
    assertFalse(result);
    assertEquals(CollectionResponse.OUT_OF_RANGE, f.getOperationStatus()
            .getResponse());

    // TYPE_MISMATCH
    f = mc.asyncBopInsertAndGetTrimmed(kvKey, 1, null, "val", null);
    result = f.get();
    assertFalse(result);
    assertEquals(CollectionResponse.TYPE_MISMATCH, f.getOperationStatus()
            .getResponse());

    // BKEY_MISMATCH
    f = mc.asyncBopInsertAndGetTrimmed(key, byteArrayBkeys[0], null, "val",
            null);
    result = f.get();
    assertFalse(result);
    assertEquals(CollectionResponse.BKEY_MISMATCH, f.getOperationStatus()
            .getResponse());

    // ELEMENT_EXISTS
    f = mc.asyncBopInsertAndGetTrimmed(key, longBkeys[0], null, "val", null);
    result = f.get();
    assertFalse(result);
    assertEquals(CollectionResponse.ELEMENT_EXISTS, f.getOperationStatus()
            .getResponse());
  }

  @Test
  public void testUpsertAndGetTrimmedOtherResponses() throws Exception {
    // insert test data
    CollectionAttributes attrs = new CollectionAttributes();
    attrs.setMaxCount(10);
    attrs.setOverflowAction(CollectionOverflowAction.smallest_trim);
    for (long each : longBkeys) {
      mc.asyncBopInsert(key, each, null, "val", attrs).get();
    }

    // set a test key
    mc.set(kvKey, 0, "value").get();

    BTreeStoreAndGetFuture<Boolean, Object> f = null;
    Boolean result = null;

    // NOT_FOUND
    f = mc.asyncBopUpsertAndGetTrimmed(invalidKey, 1, null, "val", null);
    result = f.get();
    assertFalse(result);
    assertEquals(CollectionResponse.NOT_FOUND, f.getOperationStatus()
            .getResponse());

    // OUT_OF_RANGE
    f = mc.asyncBopUpsertAndGetTrimmed(key, 1, null, "val", null);
    result = f.get();
    assertFalse(result);
    assertEquals(CollectionResponse.OUT_OF_RANGE, f.getOperationStatus()
            .getResponse());

    // TYPE_MISMATCH
    f = mc.asyncBopUpsertAndGetTrimmed(kvKey, 1, null, "val", null);
    result = f.get();
    assertFalse(result);
    assertEquals(CollectionResponse.TYPE_MISMATCH, f.getOperationStatus()
            .getResponse());

    // BKEY_MISMATCH
    f = mc.asyncBopUpsertAndGetTrimmed(key, byteArrayBkeys[0], null, "val",
            null);
    result = f.get();
    assertFalse(result);
    assertEquals(CollectionResponse.BKEY_MISMATCH, f.getOperationStatus()
            .getResponse());

    // REPLACED
    f = mc.asyncBopUpsertAndGetTrimmed(key, longBkeys[0], null, "val", null);
    result = f.get();
    assertTrue(result);
    assertEquals(CollectionResponse.REPLACED, f.getOperationStatus()
            .getResponse());
    assertNull(f.getElement());
  }

}
