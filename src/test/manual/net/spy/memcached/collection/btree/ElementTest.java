package net.spy.memcached.collection.btree;

import net.spy.memcached.collection.Element;
import net.spy.memcached.collection.ElementFlagUpdate;
import net.spy.memcached.util.BTreeUtil;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

public class ElementTest {

  private static final String VALUE = "testValue";

  @Test
  public void createWithLongBkeyAndEFlag() {
    //given
    long bkey = 1;
    byte[] eflag = {0x34};

    final Element<String> element = new Element<>(bkey, VALUE, eflag);

    //when, then
    assertEquals(bkey, element.getLongBkey());
    assertEquals(String.valueOf(bkey), element.getStringBkey());
    assertThrows(IllegalStateException.class, () -> element.getByteArrayBkey());
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

    final Element<String> element = new Element<>(bkey, VALUE, eflag);

    //when, then
    assertEquals(bkey, element.getByteArrayBkey());
    assertEquals(BTreeUtil.toHex(bkey), element.getStringBkey());
    assertThrows(IllegalStateException.class, () -> element.getLongBkey());
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

    final Element<String> element = new Element<>(bkey, VALUE, elementFlagUpdate);

    //when, then
    assertEquals(bkey, element.getLongBkey());
    assertThrows(IllegalStateException.class, () -> element.getByteArrayBkey());
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

    final Element<String> element = new Element<>(bkey, VALUE, elementFlagUpdate);

    //when, then
    assertEquals(bkey, element.getByteArrayBkey());
    assertThrows(IllegalStateException.class, () -> element.getLongBkey());
    assertEquals(VALUE, element.getValue());
    assertNull(element.getEFlag());
    assertEquals("", element.getStringEFlag());
    assertEquals(elementFlagUpdate, element.getElementFlagUpdate());
  }

}
