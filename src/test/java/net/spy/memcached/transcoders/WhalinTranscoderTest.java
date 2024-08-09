// Copyright (c) 2006  Dustin Sallings <dustin@spy.net>

package net.spy.memcached.transcoders;

import java.util.Arrays;
import java.util.Calendar;

import net.spy.memcached.CachedData;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Test the serializing transcoder.
 */
public class WhalinTranscoderTest extends BaseTranscoderCase {

  private WhalinTranscoder tc;
  private TranscoderUtils tu;

  @BeforeEach
  @Override
  public void setUp() throws Exception {
    super.setUp();
    tc = new WhalinTranscoder();
    setTranscoder(tc);
    tu = new TranscoderUtils(false);
  }

  @Test
  public void testNonserializable() throws Exception {
    try {
      tc.encode(new Object());
      fail("Processed a non-serializable object.");
    } catch (IllegalArgumentException e) {
      // pass
    }
  }

  @Test
  public void testCompressedStringNotSmaller() throws Exception {
    String s1 = "This is a test simple string that will not be compressed.";
    // Reduce the compression threshold so it'll attempt to compress it.
    tc.setCompressionThreshold(8);
    CachedData cd = tc.encode(s1);
    // This should *not* be compressed because it is too small
    assertEquals(WhalinTranscoder.SPECIAL_STRING, cd.getFlags());
    assertTrue(Arrays.equals(s1.getBytes(), cd.getData()));
    assertEquals(s1, tc.decode(cd));
  }

  @Test
  public void testCompressedString() throws Exception {
    // This one will actually compress
    String s1 = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    tc.setCompressionThreshold(8);
    CachedData cd = tc.encode(s1);
    assertEquals(
            WhalinTranscoder.COMPRESSED | WhalinTranscoder.SPECIAL_STRING,
            cd.getFlags());
    assertFalse(Arrays.equals(s1.getBytes(), cd.getData()));
    assertEquals(s1, tc.decode(cd));
  }

  @Test
  public void testObject() throws Exception {
    Calendar c = Calendar.getInstance();
    CachedData cd = tc.encode(c);
    assertEquals(WhalinTranscoder.SERIALIZED, cd.getFlags());
    assertEquals(c, tc.decode(cd));
  }

  @Test
  public void testCompressedObject() throws Exception {
    tc.setCompressionThreshold(8);
    Calendar c = Calendar.getInstance();
    CachedData cd = tc.encode(c);
    assertEquals(WhalinTranscoder.SERIALIZED
            | WhalinTranscoder.COMPRESSED, cd.getFlags());
    assertEquals(c, tc.decode(cd));
  }

  @Test
  public void testUnencodeable() throws Exception {
    try {
      CachedData cd = tc.encode(new Object());
      fail("Should fail to serialize, got" + cd);
    } catch (IllegalArgumentException e) {
      // pass
    }
  }

  @Test
  public void testUndecodeable() throws Exception {
    CachedData cd = new CachedData(
            Integer.MAX_VALUE &
                    ~(WhalinTranscoder.COMPRESSED | WhalinTranscoder.SERIALIZED),
            tu.encodeInt(Integer.MAX_VALUE),
            tc.getMaxSize());
    assertNull(tc.decode(cd));
  }

  @Test
  public void testUndecodeableSerialized() throws Exception {
    CachedData cd = new CachedData(WhalinTranscoder.SERIALIZED,
            tu.encodeInt(Integer.MAX_VALUE),
            tc.getMaxSize());
    assertNull(tc.decode(cd));
  }

  @Test
  public void testUndecodeableCompressed() throws Exception {
    CachedData cd = new CachedData(WhalinTranscoder.COMPRESSED,
            tu.encodeInt(Integer.MAX_VALUE),
            tc.getMaxSize());
    assertNull(tc.decode(cd));
  }

  @Override
  protected int getStringFlags() {
    return WhalinTranscoder.SPECIAL_STRING;
  }

}
