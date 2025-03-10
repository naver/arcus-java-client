package net.spy.memcached.collection.btree;

import net.spy.memcached.collection.BKeyObject;
import net.spy.memcached.collection.ByteArrayBKey;
import net.spy.memcached.util.BTreeUtil;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class BKeyObjectTest {

  public final static long longBkey = 1;
  public final static byte[] byteArrayBkey = {0x35};
  public final static byte[] byteArrayBkeyDiff = {0x36};

  @Test
  void createLongBkey() {
    final BKeyObject bKeyObject = new BKeyObject(longBkey);

    assertEquals(longBkey, (long) bKeyObject.getLongBKey());
    assertTrue(bKeyObject.isLong());
    assertEquals(String.valueOf(longBkey), bKeyObject.toString());
    assertEquals(BKeyObject.BKeyType.LONG, bKeyObject.getType());
    assertFalse(bKeyObject.isByteArray());
    assertThrows(IllegalStateException.class, () -> bKeyObject.getByteArrayBKeyRaw());
    assertThrows(IllegalStateException.class, () -> bKeyObject.getByteArrayBKey());
  }

  @Test
  void createByteArrayBkey() {
    final BKeyObject bKeyObject = new BKeyObject(byteArrayBkey);

    assertEquals(byteArrayBkey, bKeyObject.getByteArrayBKeyRaw());
    assertTrue(bKeyObject.isByteArray());
    assertEquals(BTreeUtil.toHex(byteArrayBkey), bKeyObject.toString());
    assertEquals(BKeyObject.BKeyType.BYTEARRAY, bKeyObject.getType());
    assertFalse(bKeyObject.isLong());
    assertThrows(IllegalStateException.class, () -> bKeyObject.getLongBKey());
  }

  @Test
  void compareLongBkeyTest() {
    BKeyObject bKeyObject = new BKeyObject(longBkey);
    BKeyObject another = new BKeyObject(longBkey);

    assertEquals(0, bKeyObject.compareTo(another));
  }

  @Test
  void compareByteArrayBkeyTest() {
    BKeyObject bKeyObject = new BKeyObject(byteArrayBkey);
    BKeyObject another = new BKeyObject(byteArrayBkey);

    assertEquals(0, bKeyObject.compareTo(another));
  }

  @Test
  void compareDifferentTypeTest() {
    final BKeyObject bKeyObject = new BKeyObject(longBkey);
    final BKeyObject another = new BKeyObject(byteArrayBkey);

    assertThrows(IllegalArgumentException.class, () -> bKeyObject.compareTo(another));
  }

  @Test
  void bkeyObjectEqualsAndHashCodeTest() {
    // given
    BKeyObject longBKey1 = new BKeyObject(longBkey);
    BKeyObject longBKey2 = new BKeyObject(longBkey);
    BKeyObject byteArrayBKey1 = new BKeyObject(byteArrayBkey);
    BKeyObject byteArrayBKey2 = new BKeyObject(byteArrayBkey);
    BKeyObject differentBKey = new BKeyObject(byteArrayBkeyDiff);

    // when, then
    assertEquals(longBKey1, longBKey2);
    assertEquals(longBKey1.hashCode(), longBKey2.hashCode());

    assertEquals(byteArrayBKey1, byteArrayBKey2);
    assertEquals(byteArrayBKey1.hashCode(), byteArrayBKey2.hashCode());

    assertNotEquals(byteArrayBKey1, differentBKey);
    assertNotEquals(byteArrayBKey1.hashCode(), differentBKey.hashCode());

    assertNotEquals(longBKey1, byteArrayBKey1);
  }

  @Test
  void byteArrayBKeyEqualsAndHashCodeTest() {
    // given
    ByteArrayBKey byteArrayBKey1 = new ByteArrayBKey(byteArrayBkey);
    ByteArrayBKey byteArrayBKey2 = new ByteArrayBKey(byteArrayBkey);
    ByteArrayBKey differentByteArrayBKey = new ByteArrayBKey(byteArrayBkeyDiff);

    // when, then
    assertEquals(byteArrayBKey1, byteArrayBKey2);
    assertEquals(byteArrayBKey1.hashCode(), byteArrayBKey2.hashCode());

    assertNotEquals(byteArrayBKey1, differentByteArrayBKey);
    assertNotEquals(byteArrayBKey1.hashCode(), differentByteArrayBKey.hashCode());
  }
}
