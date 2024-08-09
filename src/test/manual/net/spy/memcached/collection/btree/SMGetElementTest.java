package net.spy.memcached.collection.btree;

import net.spy.memcached.collection.SMGetElement;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class SMGetElementTest {

  private static final String KEY = "test";
  private static final String VALUE = "testValue";
  private static final byte[] EFLAG = {1, 8, 16, 32, 64};

  @Test
  public void createWithLongBkey() {
    //given
    long bkey = 1;

    final SMGetElement<String> element = new SMGetElement<>(KEY, bkey, EFLAG, VALUE);

    //when, then
    assertEquals(KEY, element.getKey());
    assertEquals(bkey, element.getBkey());
    assertArrayEquals(EFLAG, element.getEflag());
    assertThrows(IllegalStateException.class, () -> element.getByteBkey());
    assertEquals(VALUE, element.getValue());
  }

  @Test
  public void createWithByteArrayBkey() {
    //given
    byte[] bkey = {0x34};

    final SMGetElement<String> element = new SMGetElement<>(KEY, bkey, EFLAG, VALUE);

    //when, then
    assertEquals(KEY, element.getKey());
    assertEquals(bkey, element.getByteBkey());
    assertArrayEquals(EFLAG, element.getEflag());
    assertThrows(IllegalStateException.class, () -> element.getBkey());
    assertEquals(VALUE, element.getValue());
  }

  @Test
  public void compareToTest() {
    //given
    long bkey = 2;

    SMGetElement<String> element = new SMGetElement<>(KEY, bkey, EFLAG, VALUE);
    SMGetElement<String> anotherElement = new SMGetElement<>(KEY, bkey, EFLAG, VALUE);

    //when, then
    assertEquals(0, element.compareTo(anotherElement));
  }

  @Test
  public void compareBkeyToTest() {
    //given
    long bkey = 2;
    long anotherBkey = 1;

    SMGetElement<String> element = new SMGetElement<>(KEY, bkey, EFLAG, VALUE);
    SMGetElement<String> anotherElement = new SMGetElement<>(KEY, anotherBkey, EFLAG, VALUE);

    //when, then
    assertTrue(element.compareBkeyTo(anotherElement) > 0);
  }

}
