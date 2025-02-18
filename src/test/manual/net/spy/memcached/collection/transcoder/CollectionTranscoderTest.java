package net.spy.memcached.collection.transcoder;

import java.util.Map;
import java.util.concurrent.ExecutionException;

import net.spy.memcached.collection.BaseIntegrationTest;
import net.spy.memcached.collection.CollectionAttributes;
import net.spy.memcached.transcoders.CollectionTranscoder;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CollectionTranscoderTest extends BaseIntegrationTest {

  private static final String KEY = "CollectionTranscoderTest";

  private final CollectionTranscoder transcoder = new CollectionTranscoder.Builder()
          .disableOptimization()
          .build();

  @AfterEach
  @Override
  protected void tearDown() throws Exception {
    mc.delete(KEY);
    super.tearDown();
  }

  @Test
  void decodeIntegerAsString() throws ExecutionException, InterruptedException {
    // given
    Boolean b1 = mc.asyncMopInsert(KEY, "mkey1", "value", new CollectionAttributes()).get();
    Boolean b2 = mc.asyncMopInsert(KEY, "mkey2", 35, new CollectionAttributes()).get();
    assertTrue(b1);
    assertTrue(b2);

    // when
    Map<String, Object> map1 = mc.asyncMopGet(KEY, "mkey1", false, false).get();
    Map<String, Object> map2 = mc.asyncMopGet(KEY, "mkey2", false, false).get();

    // then
    assertEquals("value", map1.get("mkey1"));
    assertEquals("#", map2.get("mkey2"));
  }

  @Test
  void failToDecodeString() throws ExecutionException, InterruptedException {
    // given
    Boolean b2 = mc.asyncMopInsert(KEY, "mkey3", 35, new CollectionAttributes())
            .get();
    Boolean b1 = mc.asyncMopInsert(KEY, "mkey4", "value", new CollectionAttributes())
            .get();
    assertTrue(b1);
    assertTrue(b2);

    // when & then
    Map<String, Object> map1 = mc.asyncMopGet(KEY, "mkey3", false, false).get();
    assertEquals(35, map1.get("mkey3"));
    assertThrows(AssertionError.class, () -> mc.asyncMopGet(KEY, "mkey4", false, false).get());
  }

  @Test
  void decodeStringAndInteger() throws ExecutionException, InterruptedException {
    // given
    Boolean b1 = mc.asyncMopInsert(KEY, "mkey1", "value", new CollectionAttributes(), transcoder)
            .get();
    Boolean b2 = mc.asyncMopInsert(KEY, "mkey2", 35, new CollectionAttributes(), transcoder)
            .get();
    assertTrue(b1);
    assertTrue(b2);

    // when
    Map<String, Object> map1 = mc.asyncMopGet(KEY, "mkey1", false, false, transcoder).get();
    Map<String, Object> map2 = mc.asyncMopGet(KEY, "mkey2", false, false, transcoder).get();

    // then
    assertEquals("value", map1.get("mkey1"));
    assertEquals(35, map2.get("mkey2"));
  }

  @Test
  void decodeIntegerAndString() throws ExecutionException, InterruptedException {
    // given
    Boolean b2 = mc.asyncMopInsert(KEY, "mkey3", 35, new CollectionAttributes(), transcoder)
            .get();
    Boolean b1 = mc.asyncMopInsert(KEY, "mkey4", "value", new CollectionAttributes(), transcoder)
            .get();
    assertTrue(b1);
    assertTrue(b2);

    // when
    Map<String, Object> map1 = mc.asyncMopGet(KEY, "mkey3", false, false, transcoder).get();
    Map<String, Object> map2 = mc.asyncMopGet(KEY, "mkey4", false, false, transcoder).get();

    // then
    assertEquals(35, map1.get("mkey3"));
    assertEquals("value", map2.get("mkey4"));
  }
}
