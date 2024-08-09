package net.spy.memcached.transcoders;

import net.spy.memcached.CachedData;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * Test the long transcoder.
 */
public class LongTranscoderTest {

  private LongTranscoder tc = null;

  @BeforeEach
  protected void setUp() throws Exception {
    tc = new LongTranscoder();
  }

  @Test
  public void testLong() throws Exception {
    assertEquals(923, tc.decode(tc.encode(923L)).longValue());
  }

  @Test
  public void testBadFlags() throws Exception {
    CachedData cd = tc.encode(9284L);
    assertNull(tc.decode(new CachedData(cd.getFlags() + 1, cd.getData(),
            CachedData.MAX_SIZE)));
  }
}
