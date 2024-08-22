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

import java.util.concurrent.TimeUnit;

import net.spy.memcached.collection.BTreeOrder;
import net.spy.memcached.collection.BaseIntegrationTest;
import net.spy.memcached.collection.CollectionAttributes;
import net.spy.memcached.collection.CollectionResponse;
import net.spy.memcached.internal.CollectionFuture;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

public class BopFindPositionTest extends BaseIntegrationTest {

  private String key = "BopFindPositionTest";
  private String invalidKey = "InvalidBopFindPositionTest";
  private String kvKey = "KvBopFindPositionTest";

  private long[] longBkeys = {
      10L, 11L, 12L, 13L, 14L, 15L, 16L, 17L, 18L, 19L
  };
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
    mc.delete(key).get(1000, TimeUnit.MILLISECONDS);
  }

  @Test
  public void testLongBKeyAsc() throws Exception {
    // insert
    CollectionAttributes attrs = new CollectionAttributes();
    for (long each : longBkeys) {
      mc.asyncBopInsert(key, each, null, "val", attrs).get();
    }

    // bop position
    for (int i = 0; i < longBkeys.length; i++) {
      CollectionFuture<Integer> f = mc.asyncBopFindPosition(key,
              longBkeys[i], BTreeOrder.ASC);
      Integer position = f.get();
      assertNotNull(position);
      assertEquals(CollectionResponse.OK, f.getOperationStatus()
              .getResponse());
      assertEquals(i, position.intValue());
    }
  }

  @Test
  public void testLongBKeyDesc() throws Exception {
    // insert
    CollectionAttributes attrs = new CollectionAttributes();
    for (long each : longBkeys) {
      mc.asyncBopInsert(key, each, null, "val", attrs).get();
    }

    // bop position
    for (int i = 0; i < longBkeys.length; i++) {
      CollectionFuture<Integer> f = mc.asyncBopFindPosition(key,
              longBkeys[i], BTreeOrder.DESC);
      Integer position = f.get();
      assertNotNull(position);
      assertEquals(CollectionResponse.OK, f.getOperationStatus()
              .getResponse());
      assertEquals(longBkeys.length - i - 1,
              position.intValue(), "invalid position");
    }
  }

  @Test
  public void testByteArrayBKeyAsc() throws Exception {
    // insert
    CollectionAttributes attrs = new CollectionAttributes();
    for (byte[] each : byteArrayBkeys) {
      mc.asyncBopInsert(key, each, null, "val", attrs).get();
    }

    // bop position
    for (int i = 0; i < byteArrayBkeys.length; i++) {
      CollectionFuture<Integer> f = mc.asyncBopFindPosition(key,
              byteArrayBkeys[i], BTreeOrder.ASC);
      Integer position = f.get();
      assertNotNull(position);
      assertEquals(CollectionResponse.OK, f.getOperationStatus()
              .getResponse());
      assertEquals(i, position.intValue());
    }
  }

  @Test
  public void testByteArrayBKeyDesc() throws Exception {
    // insert
    CollectionAttributes attrs = new CollectionAttributes();
    for (byte[] each : byteArrayBkeys) {
      mc.asyncBopInsert(key, each, null, "val", attrs).get();
    }

    // bop position
    for (int i = 0; i < byteArrayBkeys.length; i++) {
      CollectionFuture<Integer> f = mc.asyncBopFindPosition(key,
              byteArrayBkeys[i], BTreeOrder.DESC);
      Integer position = f.get();
      assertNotNull(position);
      assertEquals(CollectionResponse.OK, f.getOperationStatus()
              .getResponse());
      assertEquals(longBkeys.length - i - 1,
              position.intValue(), "invalid position");
    }
  }

  @Test
  public void testUnsuccessfulResponses() throws Exception {
    mc.delete(invalidKey).get();
    mc.delete(kvKey).get();

    // insert
    CollectionAttributes attrs = new CollectionAttributes();
    attrs.setReadable(false);
    for (byte[] each : byteArrayBkeys) {
      mc.asyncBopInsert(key, each, null, "val", attrs).get();
    }

    // set a test key
    mc.set(kvKey, 0, "value").get();

    CollectionFuture<Integer> f = null;
    Integer position = null;

    // NOT_FOUND
    f = mc.asyncBopFindPosition(invalidKey, byteArrayBkeys[0],
            BTreeOrder.ASC);
    position = f.get();
    assertNull(position);
    assertEquals(CollectionResponse.NOT_FOUND, f.getOperationStatus()
            .getResponse());

    // UNREADABLE
    f = mc.asyncBopFindPosition(key, byteArrayBkeys[0], BTreeOrder.ASC);
    position = f.get();
    assertNull(position);
    assertEquals(CollectionResponse.UNREADABLE, f.getOperationStatus()
            .getResponse());

    attrs.setReadable(true);
    mc.asyncSetAttr(key, attrs).get();

    // BKEY_MISMATCH
    f = mc.asyncBopFindPosition(key, longBkeys[0], BTreeOrder.ASC);
    position = f.get();
    assertNull(position);
    assertEquals(CollectionResponse.BKEY_MISMATCH, f.getOperationStatus()
            .getResponse());

    // TYPE_MISMATCH
    f = mc.asyncBopFindPosition(kvKey, byteArrayBkeys[0], BTreeOrder.ASC);
    position = f.get();
    assertNull(position);
    assertEquals(CollectionResponse.TYPE_MISMATCH, f.getOperationStatus()
            .getResponse());

    // NOT_FOUND_ELEMENT
    byte[] invalidBkey = new byte[]{64};
    f = mc.asyncBopFindPosition(key, invalidBkey, BTreeOrder.ASC);
    position = f.get();
    assertNull(position);
    assertEquals(CollectionResponse.NOT_FOUND_ELEMENT, f
            .getOperationStatus().getResponse());
  }

}
