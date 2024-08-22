package net.spy.memcached.collection.btree;

import net.spy.memcached.collection.SMGetTrimKey;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class SMGetTrimKeyTest {

  private static final String KEY = "test";

  @Test
  public void createWithLongBkey() {
    //given
    long bkey = 1;
    final SMGetTrimKey trimKey = new SMGetTrimKey(KEY, bkey);

    //when, then
    assertEquals(KEY, trimKey.getKey());
    assertEquals(bkey, trimKey.getBkey());
    assertThrows(IllegalStateException.class, () -> trimKey.getByteBkey());
  }

  @Test
  public void createWithByteArrayBkey() {
    //given
    byte[] bkey = {0x34};
    final SMGetTrimKey trimKey = new SMGetTrimKey(KEY, bkey);

    //when, then
    assertEquals(KEY, trimKey.getKey());
    assertEquals(bkey, trimKey.getByteBkey());
    assertThrows(IllegalStateException.class, () -> trimKey.getBkey());
  }

  @Test
  public void compareToTest() {
    //given
    long bkey = 2;

    SMGetTrimKey trimKey = new SMGetTrimKey(KEY, bkey);
    SMGetTrimKey anotherTrimKey = new SMGetTrimKey(KEY, bkey);

    //when, then
    assertEquals(0, trimKey.compareTo(anotherTrimKey));
  }
}
