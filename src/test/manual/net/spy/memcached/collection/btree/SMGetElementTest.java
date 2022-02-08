package net.spy.memcached.collection.btree;

import net.spy.memcached.collection.SMGetElement;
import org.junit.Test;
import org.junit.function.ThrowingRunnable;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class SMGetElementTest {

  private static final String KEY = "test";
  private static final String VALUE = "testValue";

  @Test
  public void createWithLongBkey() {
    //given
    long bkey = 1;

    final SMGetElement<String> element = new SMGetElement<String>(KEY, bkey, VALUE);

    //when, then
    assertEquals(KEY, element.getKey());
    assertEquals(bkey, element.getBkey());
    assertThrows(IllegalStateException.class, new ThrowingRunnable() {
      @Override
      public void run() throws Throwable {
        element.getByteBkey();
      }
    });
    assertEquals(VALUE, element.getValue());
  }

  @Test
  public void createWithByteArrayBkey() {
    //given
    byte[] bkey = {0x34};

    final SMGetElement<String> element = new SMGetElement<String>(KEY, bkey, VALUE);

    //when, then
    assertEquals(KEY, element.getKey());
    assertEquals(bkey, element.getByteBkey());
    assertThrows(IllegalStateException.class, new ThrowingRunnable() {
      @Override
      public void run() throws Throwable {
        element.getBkey();
      }
    });
    assertEquals(VALUE, element.getValue());
  }

  @Test
  public void compareToTest() {
    //given
    long bkey = 2;

    SMGetElement<String> element = new SMGetElement<String>(KEY, bkey, VALUE);
    SMGetElement<String> anotherElement = new SMGetElement<String>(KEY, bkey, VALUE);

    //when, then
    assertEquals(0, element.compareTo(anotherElement));
  }

  @Test
  public void compareBkeyToTest() {
    //given
    long bkey = 2;
    long anotherBkey = 1;

    SMGetElement<String> element = new SMGetElement<String>(KEY, bkey, VALUE);
    SMGetElement<String> anotherElement = new SMGetElement<String>(KEY, anotherBkey, VALUE);

    //when, then
    assertTrue(element.compareBkeyTo(anotherElement) > 0);
  }

}
