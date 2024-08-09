package net.spy.memcached.LongKeyTest;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import net.spy.memcached.collection.BTreeGetResult;
import net.spy.memcached.collection.BaseIntegrationTest;
import net.spy.memcached.collection.CollectionAttributes;
import net.spy.memcached.collection.Element;
import net.spy.memcached.collection.ElementFlagFilter;
import net.spy.memcached.collection.SMGetElement;
import net.spy.memcached.collection.SMGetMode;
import net.spy.memcached.internal.BulkFuture;
import net.spy.memcached.internal.CollectionGetBulkFuture;
import net.spy.memcached.internal.SMGetFuture;
import net.spy.memcached.transcoders.LongTranscoder;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class BaseLongKeyTest extends BaseIntegrationTest {

  private final int keySize = 200;
  private final List<String> keys = new ArrayList<>();

  public BaseLongKeyTest() {
    int defaultKeyLength = 4000 - 10; // MAX_KEY_LENGTH - key index string length
    char[] chars = "abcdefghijklmnopqrstuvwxyz".toCharArray();

    for (int i = 0; i < keySize; i++) {
      StringBuilder sb = new StringBuilder();
      Random random = new Random();
      for (int j = 0; j < defaultKeyLength; j++) {
        char c = chars[random.nextInt(chars.length)];
        sb.append(c);
      }
      sb.append(i);
      keys.add(sb.toString());
    }
  }

  @Test
  public void testKV_Long() throws Exception {
    // KV Set
    assertTrue(mc.set(keys.get(0), 10, "value1").get());

    // KV Get
    assertEquals("value1", mc.asyncGet(keys.get(0)).get());

    // Delete Key
    assertTrue(mc.delete(keys.get(0)).get());
  }

  @Test
  public void testSet_Long() throws Exception {
    // Set Collection Create & Insert
    assertTrue(mc.asyncSopInsert(keys.get(0), 10L, new CollectionAttributes()).get());

    // Set Collection Get
    Set<Long> rlist = mc.asyncSopGet(keys.get(0), 10, false, false,
            new LongTranscoder()).get();
    assertTrue(rlist.contains(10L));

    // Set Collection Element Delete
    assertTrue(mc.asyncSopDelete(keys.get(0), 10L, false).get());

    // Delete Key
    assertTrue(mc.delete(keys.get(0)).get());
  }

  @Test
  public void testMap_Long() throws Exception {
    // Map Collection Create & Insert
    assertTrue(mc.asyncMopInsert(keys.get(0), "mkey1", 10L, new CollectionAttributes()).get());

    // Map Collection Get
    Map<String, Long> rmap = mc.asyncMopGet(keys.get(0), false, false,
            new LongTranscoder()).get();
    assertEquals((Long) 10L, rmap.get("mkey1"));

    // Map Collection Element Delete
    assertTrue(mc.asyncMopDelete(keys.get(0), false).get());

    // Delete Key
    assertTrue(mc.delete(keys.get(0)).get());
  }

  @Test
  public void testList_Long() throws Exception {
    // List Collection Create & Insert
    assertTrue(mc.asyncLopInsert(keys.get(0), 0, 10L, new CollectionAttributes()).get());

    // List Collection Get
    List<Long> rlist = mc.asyncLopGet(keys.get(0), 0, 10, false, false,
            new LongTranscoder()).get();
    assertEquals(10L, rlist.get(0).longValue());

    // List Collection Element Delete
    assertTrue(mc.asyncLopDelete(keys.get(0), 0, 10, false).get());

    // Delete Key
    assertTrue(mc.delete(keys.get(0)).get());
  }

  @Test
  public void testBtree_Long() throws Exception {
    // BTree Collection Create & Insert
    assertTrue(mc.asyncBopInsert(keys.get(0), 10, null, 10L,
            new CollectionAttributes()).get());

    // BTree Collection Get
    Map<Long, Element<Long>> rmap = mc.asyncBopGet(keys.get(0), 0, 100,
            ElementFlagFilter.DO_NOT_FILTER, 0, 0, false, false,
            new LongTranscoder()).get();
    assertEquals((Long) 10L, rmap.get(10L).getValue());

    // Btree Collection Delete
    assertTrue(mc.asyncBopDelete(keys.get(0), 10, ElementFlagFilter.DO_NOT_FILTER, false).get());

    // Delete Key
    assertTrue(mc.delete(keys.get(0)).get());
  }

  @Test
  public void testKV_BulkGet_Long() throws Exception {
    // KV Set
    for (int i = 0; i < keySize; i++) {
      assertTrue(mc.set(keys.get(i), 60, "value" + i).get());
    }

    // KV BulkGet
    BulkFuture<Map<String, Object>> f = mc.asyncGetBulk(keys);
    Map<String, Object> results = f.get();

    for (int i = 0; i < keySize; i++) {
      assertEquals("value" + i, results.get(keys.get(i)));
    }

    // Delete Key
    for (int i = 0; i < keySize; i++) {
      assertTrue(mc.delete(keys.get(i)).get());
    }
  }

  @Test
  public void testBtree_MGet_Long() throws Exception {
    // BTree Collection Create & Insert
    for (int i = 0; i < keySize; i++) {
      assertTrue(mc.asyncBopInsert(keys.get(i), 10, null, 10L,
              new CollectionAttributes()).get());
    }

    // Btree Collection BulkGet
    CollectionGetBulkFuture<Map<String, BTreeGetResult<Long, Object>>> f = mc
            .asyncBopGetBulk(keys, 0, 100, ElementFlagFilter.DO_NOT_FILTER, 0, 10);
    Map<String, BTreeGetResult<Long, Object>> results = f.get();
    assertEquals(keySize, results.size());

    // Delete Key
    for (int i = 0; i < keySize; i++) {
      assertTrue(mc.delete(keys.get(i)).get());
    }
  }

  @Test
  public void testBtree_SMGet_Long() throws Exception {
    // BTree Collection Create & Insert
    for (int i = 0; i < keySize; i++) {
      assertTrue(mc.asyncBopInsert(keys.get(i), i, null, "VALUE" + i,
              new CollectionAttributes()).get());
    }
    SMGetMode smgetMode = SMGetMode.UNIQUE;

    /* old SMGetTest */
    SMGetFuture<List<SMGetElement<Object>>> oldFuture = mc
            .asyncBopSortMergeGet(keys, 0, 10,
                    ElementFlagFilter.DO_NOT_FILTER, 0, 10);
    try {
      List<SMGetElement<Object>> map = oldFuture
              .get();

      assertEquals(10, map.size());
      assertTrue(oldFuture.getMissedKeyList().isEmpty());

      for (int i = 0; i < map.size(); i++) {
        assertEquals(keys.get(i), map.get(i).getKey());
        assertEquals(i, map.get(i).getBkey());
        assertEquals("VALUE" + i, map.get(i).getValue());
      }
    } catch (Exception e) {
      oldFuture.cancel(true);
      e.printStackTrace();
      Assertions.fail(e.getMessage());
    }

    SMGetFuture<List<SMGetElement<Object>>> future = mc
            .asyncBopSortMergeGet(keys, 0, 10,
                    ElementFlagFilter.DO_NOT_FILTER, 10, smgetMode);
    try {
      List<SMGetElement<Object>> map = future
              .get();

      assertEquals(10, map.size());
      assertTrue(future.getMissedKeys().isEmpty());

      for (int i = 0; i < map.size(); i++) {
        assertEquals(keys.get(i), map.get(i).getKey());
        assertEquals(i, map.get(i).getBkey());
        assertEquals("VALUE" + i, map.get(i).getValue());
      }
    } catch (Exception e) {
      future.cancel(true);
      e.printStackTrace();
      Assertions.fail(e.getMessage());
    }

    // Delete Key
    for (int i = 0; i < keySize; i++) {
      assertTrue(mc.delete(keys.get(i)).get());
    }
  }
}
