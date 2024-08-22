package net.spy.memcached.transcoders;

import net.spy.memcached.CachedData;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Test a couple aspects of CachedData.
 */
public class CachedDataTest {

  @Test
  public void testToString() throws Exception {
    String exp = "{CachedData flags=13 data=[84, 104, 105, 115, 32, 105, "
            + "115, 32, 97, 32, 115, 105, 109, 112, 108, 101, 32, 116, 101, "
            + "115, 116, 32, 115, 116, 114, 105, 110, 103, 46] eFlag=null }";
    CachedData cd = new CachedData(13,
            "This is a simple test string.".getBytes("UTF-8"),
            CachedData.MAX_SIZE);
    assertEquals(exp, String.valueOf(cd));
  }

}
