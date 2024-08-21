package net.spy.memcached.transcoders;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;

import net.spy.memcached.CachedData;
import net.spy.memcached.compat.BaseMockCase;

/**
 * Basic behavior validation for all transcoders that work with objects.
 */
public abstract class BaseTranscoderCase extends BaseMockCase {

  private Transcoder<Object> tc;

  protected void setTranscoder(Transcoder<Object> t) {
    assert t != null;
    tc = t;
  }

  protected Transcoder<Object> getTranscoder() {
    return tc;
  }

  public void testSomethingBigger() throws Exception {
    Collection<Date> dates = new ArrayList<>();
    for (int i = 0; i < 1024; i++) {
      dates.add(new Date());
    }
    CachedData d = tc.encode(dates);
    assertEquals(dates, tc.decode(d));
  }

  public void testDate() throws Exception {
    Date d = new Date();
    CachedData cd = tc.encode(d);
    assertEquals(d, tc.decode(cd));
  }

  public void testLong() throws Exception {
    assertEquals(923L, tc.decode(tc.encode(923L)));
  }

  public void testInt() throws Exception {
    assertEquals(923, tc.decode(tc.encode(923)));
  }

  public void testShort() throws Exception {
    assertEquals((short) 923, tc.decode(tc.encode((short) 923)));
  }

  public void testChar() throws Exception {
    assertEquals('c', tc.decode(tc.encode('c')));
  }

  public void testBoolean() throws Exception {
    assertSame(Boolean.TRUE, tc.decode(tc.encode(true)));
    assertSame(Boolean.FALSE, tc.decode(tc.encode(false)));
  }

  public void testByte() throws Exception {
    assertEquals((byte) -127, tc.decode(tc.encode((byte) -127)));
  }

  public void testStringBuilder() throws Exception {
    StringBuilder sb = new StringBuilder("test");
    StringBuilder sb2 = (StringBuilder) tc.decode(tc.encode(sb));
    assertEquals(sb.toString(), sb2.toString());
  }

  public void testStringBuffer() throws Exception {
    StringBuffer sb = new StringBuffer("test");
    StringBuffer sb2 = (StringBuffer) tc.decode(tc.encode(sb));
    assertEquals(sb.toString(), sb2.toString());
  }


  private void assertFloat(float f) {
    assertEquals(f, tc.decode(tc.encode(f)));
  }

  public void testFloat() throws Exception {
    assertFloat(0f);
    assertFloat(Float.MIN_VALUE);
    assertFloat(Float.MAX_VALUE);
    assertFloat(3.14f);
    assertFloat(-3.14f);
    assertFloat(Float.NaN);
    assertFloat(Float.POSITIVE_INFINITY);
    assertFloat(Float.NEGATIVE_INFINITY);
  }

  private void assertDouble(double d) {
    assertEquals(d, tc.decode(tc.encode(d)));
  }

  public void testDouble() throws Exception {
    assertDouble(0d);
    assertDouble(Double.MIN_VALUE);
    assertDouble(Double.MAX_VALUE);
    assertDouble(3.14d);
    assertDouble(-3.14d);
    assertDouble(Double.NaN);
    assertDouble(Double.POSITIVE_INFINITY);
    assertDouble(Double.NEGATIVE_INFINITY);
  }

  private void assertLong(long l) {
    CachedData encoded = tc.encode(l);
    long decoded = (Long) tc.decode(encoded);
    assertEquals(l, decoded);
  }

  /*
    private void displayBytes(long l, byte[] encoded) {
      System.out.print(l + " [");
      for(byte b : encoded) {
        System.out.print((b<0?256+b:b) + " ");
      }
      System.out.println("]");
    }
    */

  public void testLongEncoding() throws Exception {
    assertLong(Long.MIN_VALUE);
    assertLong(1);
    assertLong(23852);
    assertLong(0L);
    assertLong(-1);
    assertLong(-23835);
    assertLong(Long.MAX_VALUE);
  }

  private void assertInt(int i) {
    CachedData encoded = tc.encode(i);
    int decoded = (Integer) tc.decode(encoded);
    assertEquals(i, decoded);
  }

  public void testIntEncoding() throws Exception {
    assertInt(Integer.MIN_VALUE);
    assertInt(83526);
    assertInt(1);
    assertInt(0);
    assertInt(-1);
    assertInt(-238526);
    assertInt(Integer.MAX_VALUE);
  }

  public void testBooleanEncoding() throws Exception {
    assertTrue((Boolean) tc.decode(tc.encode(true)));
    assertFalse((Boolean) tc.decode(tc.encode(false)));
  }

  public void testByteArray() throws Exception {
    byte[] a = {'a', 'b', 'c'};
    CachedData cd = tc.encode(a);
    assertTrue(Arrays.equals(a, cd.getData()));
    assertTrue(Arrays.equals(a, (byte[]) tc.decode(cd)));
  }

  public void testStrings() throws Exception {
    String s1 = "This is a simple test string.";
    CachedData cd = tc.encode(s1);
    assertEquals(getStringFlags(), cd.getFlags());
    assertEquals(s1, tc.decode(cd));
  }

  public void testUTF8String() throws Exception {
    String s1 = "\u2013\u00f3\u2013\u00a5\u2014\u00c4\u2013\u221e\u2013"
            + "\u2264\u2014\u00c5\u2014\u00c7\u2013\u2264\u2014\u00c9\u2013"
            + "\u03c0, \u2013\u00ba\u2013\u220f\u2014\u00c4.";
    CachedData cd = tc.encode(s1);
    assertEquals(getStringFlags(), cd.getFlags());
    assertEquals(s1, tc.decode(cd));
  }

  protected abstract int getStringFlags();
}
