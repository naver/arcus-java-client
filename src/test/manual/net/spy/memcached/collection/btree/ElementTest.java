package net.spy.memcached.collection.btree;

import net.spy.memcached.collection.Element;
import net.spy.memcached.collection.ElementFlagUpdate;
import net.spy.memcached.util.BTreeUtil;
import org.junit.Test;
import org.junit.function.ThrowingRunnable;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;

public class ElementTest {

  private static final String VALUE = "testValue";

  @Test
  public void createWithLongBkeyAndEFlag() {
    //given
    long bkey = 1;
    byte[] eflag = {0x34};

    final Element<String> element = new Element<String>(bkey, VALUE, eflag);

    //when, then
    assertEquals(bkey, element.getLongBkey());
    assertEquals(String.valueOf(bkey), element.getStringBkey());
    assertThrows(IllegalStateException.class, new ThrowingRunnable() {
      @Override
      public void run() throws Throwable {
        element.getByteArrayBkey();
      }
    });
    assertEquals(VALUE, element.getValue());
    assertEquals(eflag, element.getEFlag());
    assertEquals(BTreeUtil.toHex(eflag), element.getStringEFlag());
    assertNull(element.getElementFlagUpdate());
  }

  @Test
  public void createWithByteArrayBkeyAndEFlag() {
    //given
    byte[] bkey = {0x3F};
    byte[] eflag = {0x34};

    final Element<String> element = new Element<String>(bkey, VALUE, eflag);

    //when, then
    assertEquals(bkey, element.getByteArrayBkey());
    assertEquals(BTreeUtil.toHex(bkey), element.getStringBkey());
    assertThrows(IllegalStateException.class, new ThrowingRunnable() {
      @Override
      public void run() throws Throwable {
        element.getLongBkey();
      }
    });
    assertEquals(VALUE, element.getValue());
    assertEquals(eflag, element.getEFlag());
    assertEquals(BTreeUtil.toHex(eflag), element.getStringEFlag());
    assertNull(element.getElementFlagUpdate());
  }

  @Test
  public void createWithLongBkeyAndElementFlagUpdate() {
    //given
    long bkey = 1;
    byte[] eflag = {0x34};
    ElementFlagUpdate elementFlagUpdate = new ElementFlagUpdate(eflag);

    final Element<String> element = new Element<String>(bkey, VALUE, elementFlagUpdate);

    //when, then
    assertEquals(bkey, element.getLongBkey());
    assertThrows(IllegalStateException.class, new ThrowingRunnable() {
      @Override
      public void run() throws Throwable {
        element.getByteArrayBkey();
      }
    });
    assertEquals(VALUE, element.getValue());
    assertNull(element.getEFlag());
    assertEquals("", element.getStringEFlag());
    assertEquals(elementFlagUpdate, element.getElementFlagUpdate());
  }

  @Test
  public void createWithByteArrayBkeyAndElementFlagUpdate() {
    //given
    byte[] bkey = {0x3F};
    byte[] eflag = {0x34};
    ElementFlagUpdate elementFlagUpdate = new ElementFlagUpdate(eflag);

    final Element<String> element = new Element<String>(bkey, VALUE, elementFlagUpdate);

    //when, then
    assertEquals(bkey, element.getByteArrayBkey());
    assertThrows(IllegalStateException.class, new ThrowingRunnable() {
      @Override
      public void run() throws Throwable {
        element.getLongBkey();
      }
    });
    assertEquals(VALUE, element.getValue());
    assertNull(element.getEFlag());
    assertEquals("", element.getStringEFlag());
    assertEquals(elementFlagUpdate, element.getElementFlagUpdate());
  }

}
