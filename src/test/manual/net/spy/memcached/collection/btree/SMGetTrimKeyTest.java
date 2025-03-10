package net.spy.memcached.collection.btree;

import net.spy.memcached.collection.SMGetTrimKey;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class SMGetTrimKeyTest {

  private static final String KEY = "test";

  @Test
  void createWithLongBkey() {
    //given
    long bkey = 1;
    final SMGetTrimKey trimKey = new SMGetTrimKey(KEY, bkey);

    //when, then
    assertEquals(KEY, trimKey.getKey());
    assertEquals(bkey, trimKey.getBkey());
    assertThrows(IllegalStateException.class, () -> trimKey.getByteBkey());
  }

  @Test
  void createWithByteArrayBkey() {
    //given
    byte[] bkey = {0x34};
    final SMGetTrimKey trimKey = new SMGetTrimKey(KEY, bkey);

    //when, then
    assertEquals(KEY, trimKey.getKey());
    assertEquals(bkey, trimKey.getByteBkey());
    assertThrows(IllegalStateException.class, () -> trimKey.getBkey());
  }

  @Test
  void compareToTest() {
    //given
    long bkey = 2;

    SMGetTrimKey trimKey = new SMGetTrimKey(KEY, bkey);
    SMGetTrimKey anotherTrimKey = new SMGetTrimKey(KEY, bkey);

    //when, then
    assertEquals(0, trimKey.compareTo(anotherTrimKey));
  }

  @Test
  void equalsAndHashCodeTest() {
    //given
    SMGetTrimKey trimKey1 = new SMGetTrimKey(KEY, 1L);
    SMGetTrimKey trimKey2 = new SMGetTrimKey(KEY, 1L);
    SMGetTrimKey trimKeyWithDiffKey = new SMGetTrimKey("diffKey", 1L);
    SMGetTrimKey trimKeyWithDiffBkey = new SMGetTrimKey(KEY, 2L);

    byte[] byteBkey = {0x34};
    SMGetTrimKey trimKeyWithByteBkey1 = new SMGetTrimKey(KEY, byteBkey);
    SMGetTrimKey trimKeyWithByteBkey2 = new SMGetTrimKey(KEY, byteBkey);
    SMGetTrimKey trimKeyWithDiffByteBkey = new SMGetTrimKey(KEY, new byte[]{0x35});

    //when, then
    assertEquals(trimKey1, trimKey2);
    assertEquals(trimKey1.hashCode(), trimKey2.hashCode());

    assertNotEquals(trimKey1, trimKeyWithDiffKey);
    assertNotEquals(trimKey1, trimKeyWithDiffBkey);

    assertEquals(trimKeyWithByteBkey1, trimKeyWithByteBkey2);
    assertEquals(trimKeyWithByteBkey1.hashCode(), trimKeyWithByteBkey2.hashCode());

    assertNotEquals(trimKeyWithByteBkey1, trimKeyWithDiffByteBkey);
  }
}
