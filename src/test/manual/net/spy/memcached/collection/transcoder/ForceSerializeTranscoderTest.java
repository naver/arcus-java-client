package net.spy.memcached.collection.transcoder;

import java.io.Serializable;
import java.util.Date;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutionException;

import com.fasterxml.jackson.databind.ObjectMapper;

import net.spy.memcached.collection.BaseIntegrationTest;
import net.spy.memcached.collection.CollectionAttributes;
import net.spy.memcached.transcoders.GenericJsonSerializingTranscoder;
import net.spy.memcached.transcoders.SerializingTranscoder;
import net.spy.memcached.transcoders.Transcoder;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ForceSerializeTranscoderTest extends BaseIntegrationTest {

  private static final String KEY = "ForceSerializeTranscoderTest";

  @AfterEach
  @Override
  protected void tearDown() throws Exception {
    mc.delete(KEY);
    super.tearDown();
  }

  @Test
  void decodeIntegerAsStringWithSerializingTranscoder()
      throws ExecutionException, InterruptedException {
    Transcoder<Object> transcoder = SerializingTranscoder.forCollection().build();

    // given
    Boolean b1 = mc.asyncMopInsert(KEY, "mkey1", "value",
        new CollectionAttributes(), transcoder).get();
    Boolean b2 = mc.asyncMopInsert(KEY, "mkey2", 35,
        new CollectionAttributes(), transcoder).get();
    assertTrue(b1);
    assertTrue(b2);

    // when
    Map<String, Object> map = mc
        .asyncMopGet(KEY, false, false, transcoder).get();

    // then
    assertEquals("value", map.get("mkey1"));
    assertEquals("#", map.get("mkey2"));
  }

  @Test
  void decodeMultipleTypesWithSerializingTranscoder()
      throws ExecutionException, InterruptedException {
    Transcoder<Object> transcoder = SerializingTranscoder.forCollection()
        .forceJDKSerializationForCollection()
        .build();
    TestPojo pojo = new TestPojo("name", 100);

    // given
    Boolean b1 = mc.asyncMopInsert(KEY, "mkey1", 35,
        new CollectionAttributes(), transcoder).get();
    Boolean b2 = mc.asyncMopInsert(KEY, "mkey2", "value",
        new CollectionAttributes(), transcoder).get();
    Boolean b3 = mc.asyncMopInsert(KEY, "mkey3", pojo,
        new CollectionAttributes(), transcoder).get();
    assertTrue(b1);
    assertTrue(b2);
    assertTrue(b3);

    // when
    Map<String, Object> map = mc
        .asyncMopGet(KEY, false, false, transcoder).get();

    // then
    assertEquals(35, map.get("mkey1"));
    assertEquals("value", map.get("mkey2"));
    assertEquals(pojo, map.get("mkey3"));
  }

  @Test
  void decodeIntegerAsStringWithGenericJsonSerializingTranscoder()
      throws ExecutionException, InterruptedException {
    Transcoder<Object> transcoder = GenericJsonSerializingTranscoder
        .forCollection(new ObjectMapper())
        .build();
    TestPojo pojo = new TestPojo("name", 100);
    Date date = new Date();

    // given
    Boolean b1 = mc.asyncMopInsert(KEY, "mkey1", "value",
        new CollectionAttributes(), transcoder).get();
    Boolean b2 = mc.asyncMopInsert(KEY, "mkey2", 35,
        new CollectionAttributes(), transcoder).get();
    Boolean b3 = mc.asyncMopInsert(KEY, "mkey3", pojo,
        new CollectionAttributes(), transcoder).get();
    Boolean b4 = mc.asyncMopInsert(KEY, "mkey4", date,
        new CollectionAttributes(), transcoder).get();
    assertTrue(b1);
    assertTrue(b2);
    assertTrue(b3);
    assertTrue(b4);

    // when
    Map<String, Object> map = mc
        .asyncMopGet(KEY, false, false, transcoder).get();

    // then
    assertEquals("value", map.get("mkey1"));
    assertEquals("#", map.get("mkey2"));
    assertNotEquals(pojo, map.get("mkey3"));
    assertNotEquals(date, map.get("mkey4"));
  }

  @Test
  void decodeMultipleTypesWithGenericJsonSerializingTranscoder()
      throws ExecutionException, InterruptedException {
    Transcoder<Object> transcoder = GenericJsonSerializingTranscoder
        .forCollection(new ObjectMapper())
        .forceJsonSerializeForCollection()
        .build();
    TestPojo pojo = new TestPojo("name", 100);
    Date date = new Date();

    // given
    Boolean b1 = mc.asyncMopInsert(KEY, "mkey1", 35,
        new CollectionAttributes(), transcoder).get();
    Boolean b2 = mc.asyncMopInsert(KEY, "mkey2", "value",
        new CollectionAttributes(), transcoder).get();
    Boolean b3 = mc.asyncMopInsert(KEY, "mkey3", pojo,
        new CollectionAttributes(), transcoder).get();
    Boolean b4 = mc.asyncMopInsert(KEY, "mkey4", date,
        new CollectionAttributes(), transcoder).get();
    assertTrue(b1);
    assertTrue(b2);
    assertTrue(b3);
    assertTrue(b4);

    // when
    Map<String, Object> map = mc
        .asyncMopGet(KEY, false, false, transcoder).get();

    // then
    assertEquals(35, map.get("mkey1"));
    assertEquals("value", map.get("mkey2"));
    assertEquals(pojo, map.get("mkey3"));
    assertEquals(date, map.get("mkey4"));
  }

  public static class TestPojo implements Serializable {

    private static final long serialVersionUID = 4892462019291743546L;
    private String name;
    private int value;

    public TestPojo() {
    }

    public TestPojo(String name, int value) {
      this.name = name;
      this.value = value;
    }

    public String getName() {
      return name;
    }

    public int getValue() {
      return value;
    }

    public void setValue(int value) {
      this.value = value;
    }

    @Override
    public boolean equals(Object o) {
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      TestPojo testPojo = (TestPojo) o;
      return value == testPojo.value && Objects.equals(name, testPojo.name);
    }

    @Override
    public int hashCode() {
      return Objects.hash(name, value);
    }
  }
}
