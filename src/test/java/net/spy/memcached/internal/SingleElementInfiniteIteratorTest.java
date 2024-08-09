package net.spy.memcached.internal;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

public class SingleElementInfiniteIteratorTest {
  private static final String CONSTANT = "foo";
  private SingleElementInfiniteIterator<String> iterator;

  @BeforeEach
  protected void setUp() throws Exception {
    iterator = new SingleElementInfiniteIterator<>(CONSTANT);
  }

  @Test
  public void testHasNextAndNext() {
    for (int i = 0; i < 100; ++i) {
      assertTrue(iterator.hasNext());
      assertSame(CONSTANT, iterator.next());
    }
  }

  @Test
  public void testRemove() {
    try {
      iterator.remove();
      fail("Expected UnsupportedOperationException on a remove.");
    } catch (UnsupportedOperationException e) {
      // test success.
    }
  }
}
