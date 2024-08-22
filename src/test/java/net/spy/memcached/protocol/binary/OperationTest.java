package net.spy.memcached.protocol.binary;

import org.junit.jupiter.api.Test;

import static net.spy.memcached.protocol.binary.OperationImpl.decodeInt;
import static net.spy.memcached.protocol.binary.OperationImpl.decodeUnsignedInt;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Test operation stuff.
 */
public class OperationTest {

  @Test
  public void testIntegerDecode() {
    assertEquals(129,
            decodeInt(new byte[]{0, 0, 0, (byte) 0x81}, 0));
    assertEquals(129 * 256,
            decodeInt(new byte[]{0, 0, (byte) 0x81, 0}, 0));
    assertEquals(129 * 256 * 256,
            decodeInt(new byte[]{0, (byte) 0x81, 0, 0}, 0));
    assertEquals(129 * 256 * 256 * 256,
            decodeInt(new byte[]{(byte) 0x81, 0, 0, 0}, 0));
  }

  @Test
  public void testUnsignedIntegerDecode() {
    assertEquals(129,
            decodeUnsignedInt(new byte[]{0, 0, 0, (byte) 0x81}, 0));
    assertEquals(129 * 256,
            decodeUnsignedInt(new byte[]{0, 0, (byte) 0x81, 0}, 0));
    assertEquals(129 * 256 * 256,
            decodeUnsignedInt(new byte[]{0, (byte) 0x81, 0, 0}, 0));
    assertEquals(129L * 256L * 256L * 256L,
            decodeUnsignedInt(new byte[]{(byte) 0x81, 0, 0, 0}, 0));
  }

  @Test
  public void testOperationStatusString() {
    String s = String.valueOf(OperationImpl.STATUS_OK);
    assertEquals("{OperationStatus success=true:  OK}", s);
  }
}
