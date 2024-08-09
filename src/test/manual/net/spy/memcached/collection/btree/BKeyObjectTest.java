package net.spy.memcached.collection.btree;

import net.spy.memcached.collection.BKeyObject;
import net.spy.memcached.util.BTreeUtil;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class BKeyObjectTest {

  public final static long longBkey = 1;
  public final static byte[] byteArrayBkey = {0x35};

  @Test
  public void createLongBkey() {
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
  public void createByteArrayBkey() {
    final BKeyObject bKeyObject = new BKeyObject(byteArrayBkey);

    assertEquals(byteArrayBkey, bKeyObject.getByteArrayBKeyRaw());
    assertTrue(bKeyObject.isByteArray());
    assertEquals(BTreeUtil.toHex(byteArrayBkey), bKeyObject.toString());
    assertEquals(BKeyObject.BKeyType.BYTEARRAY, bKeyObject.getType());
    assertFalse(bKeyObject.isLong());
    assertThrows(IllegalStateException.class, () -> bKeyObject.getLongBKey());
  }

  @Test
  public void compareLongBkeyTest() {
    BKeyObject bKeyObject = new BKeyObject(longBkey);
    BKeyObject another = new BKeyObject(longBkey);

    assertEquals(0, bKeyObject.compareTo(another));
  }

  @Test
  public void compareByteArrayBkeyTest() {
    BKeyObject bKeyObject = new BKeyObject(byteArrayBkey);
    BKeyObject another = new BKeyObject(byteArrayBkey);

    assertEquals(0, bKeyObject.compareTo(another));
  }

  @Test
  public void compareDifferentTypeTest() {
    final BKeyObject bKeyObject = new BKeyObject(longBkey);
    final BKeyObject another = new BKeyObject(byteArrayBkey);

    assertThrows(IllegalArgumentException.class,
            () -> bKeyObject.compareTo(another));
  }


}
