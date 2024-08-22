package net.spy.memcached.transcoders;

import net.spy.memcached.CachedData;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * Test the integer transcoder.
 */
public class IntegerTranscoderTest {

  private IntegerTranscoder tc = null;

  @BeforeEach
  protected void setUp() throws Exception {
    tc = new IntegerTranscoder();
  }

  @Test
  public void testInt() throws Exception {
    assertEquals(923, tc.decode(tc.encode(923)).intValue());
  }

  @Test
  public void testBadFlags() throws Exception {
    CachedData cd = tc.encode(9284);
    assertNull(tc.decode(new CachedData(cd.getFlags() + 1, cd.getData(),
            CachedData.MAX_SIZE)));
  }
}
