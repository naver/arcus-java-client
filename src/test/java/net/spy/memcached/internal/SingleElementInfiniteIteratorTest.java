package net.spy.memcached.internal;

import junit.framework.TestCase;

public class SingleElementInfiniteIteratorTest extends TestCase {
  private static final String CONSTANT = "foo";
  private SingleElementInfiniteIterator<String> iterator;

  @Override
  protected void setUp() throws Exception {
    super.setUp();
    iterator = new SingleElementInfiniteIterator<>(CONSTANT);
  }

  public void testHasNextAndNext() {
    for (int i = 0; i < 100; ++i) {
      assertTrue(iterator.hasNext());
      assertSame(CONSTANT, iterator.next());
    }
  }

  public void testRemove() {
    try {
      iterator.remove();
      fail("Expected UnsupportedOperationException on a remove.");
    } catch (UnsupportedOperationException e) {
      // test success.
    }
  }
}
