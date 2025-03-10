package net.spy.memcached.collection.btree;

import net.spy.memcached.collection.SMGetElement;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SMGetElementTest {

  private static final String KEY = "test";
  private static final String VALUE = "testValue";
  private static final byte[] EFLAG = {1, 8, 16, 32, 64};
  private static final byte[] DIFF_EFLAG = {2, 4, 8, 16, 32};

  @Test
  void createWithLongBkey() {
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
  void createWithByteArrayBkey() {
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
  void compareToTest() {
    //given
    long bkey = 2;

    SMGetElement<String> element = new SMGetElement<>(KEY, bkey, EFLAG, VALUE);
    SMGetElement<String> anotherElement = new SMGetElement<>(KEY, bkey, EFLAG, VALUE);

    //when, then
    assertEquals(0, element.compareTo(anotherElement));
  }

  @Test
  void compareBkeyToTest() {
    //given
    long bkey = 2;
    long anotherBkey = 1;

    SMGetElement<String> element = new SMGetElement<>(KEY, bkey, EFLAG, VALUE);
    SMGetElement<String> anotherElement = new SMGetElement<>(KEY, anotherBkey, EFLAG, VALUE);

    //when, then
    assertTrue(element.compareBkeyTo(anotherElement) > 0);
  }

  @Test
  void equalsAndHashCodeTest() {
    //given
    SMGetElement<String> element1 = new SMGetElement<>(KEY, 1L, EFLAG, VALUE);
    SMGetElement<String> element2 = new SMGetElement<>(KEY, 1L, EFLAG, VALUE);
    SMGetElement<String> elementWithDiffEflag = new SMGetElement<>(KEY, 1L, DIFF_EFLAG, VALUE);
    SMGetElement<String> elementWithDiffKey = new SMGetElement<>("diffKey", 1L, EFLAG, VALUE);

    //when, then
    assertEquals(element1, element2);
    assertEquals(element1.hashCode(), element2.hashCode());

    assertNotEquals(element1, elementWithDiffEflag);
    assertNotEquals(element1.hashCode(), elementWithDiffEflag.hashCode());

    assertNotEquals(element1, elementWithDiffKey);
  }
}
