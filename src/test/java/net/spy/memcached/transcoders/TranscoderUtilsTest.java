package net.spy.memcached.transcoders;

import java.util.Arrays;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Some test coverage for transcoder utils.
 */
class TranscoderUtilsTest {

  private TranscoderUtils tu;
  private final byte[] oversizeBytes = new byte[16];

  @BeforeEach
  protected void setUp() throws Exception {
    tu = new TranscoderUtils(true);
  }

  @Test
  void testBooleanOverflow() {
    try {
      boolean b = tu.decodeBoolean(oversizeBytes);
      fail("Got " + b + " expected assertion.");
    } catch (AssertionError e) {
      // pass
    }
  }

  @Test
  void testByteOverflow() {
    try {
      byte b = tu.decodeByte(oversizeBytes);
      fail("Got " + b + " expected assertion.");
    } catch (AssertionError e) {
      // pass
    }
  }

  @Test
  void testIntOverflow() {
    try {
      int b = tu.decodeInt(oversizeBytes);
      fail("Got " + b + " expected assertion.");
    } catch (AssertionError e) {
      // pass
    }
  }

  @Test
  void testLongOverflow() {
    try {
      long b = tu.decodeLong(oversizeBytes);
      fail("Got " + b + " expected assertion.");
    } catch (AssertionError e) {
      // pass
    }
  }

  @Test
  void testPackedLong() {
    assertEquals("[1]", Arrays.toString(tu.encodeLong(1)));
  }

  @Test
  void testUnpackedLong() {
    assertEquals("[0, 0, 0, 0, 0, 0, 0, 1]",
            Arrays.toString(new TranscoderUtils(false).encodeLong(1)));
  }
}
