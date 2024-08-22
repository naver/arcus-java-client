package net.spy.memcached;

import java.util.Iterator;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import org.jmock.Mockery;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

public abstract class AbstractNodeLocationCase {

  protected MemcachedNode[] nodes;
  protected NodeLocator locator;
  protected Mockery context;

  @AfterEach
  protected void tearDown() throws Exception {
    context.assertIsSatisfied();
  }

  private void runSequenceAssertion(NodeLocator l, String k, int... seq) {
    int pos = 0;
    for (Iterator<MemcachedNode> i = l.getSequence(k); i.hasNext(); ) {
      assertEquals(nodes[seq[pos]].toString(), i.next().toString(),
              "At position " + pos);
      try {
        i.remove();
        fail("Allowed a removal from a sequence.");
      } catch (UnsupportedOperationException e) {
        // pass
      }
      pos++;
    }
    assertEquals(seq.length, pos, "Incorrect sequence size for " + k);
  }

  @Test
  public final void testCloningGetPrimary() {
    setupNodes(5);
    assertTrue(locator.getReadonlyCopy().getPrimary("hi")
            instanceof MemcachedNodeROImpl);
  }

  @Test
  public final void testCloningGetAll() {
    setupNodes(5);
    assertTrue(locator.getReadonlyCopy().getAll().iterator().next()
            instanceof MemcachedNodeROImpl);
  }

  @Test
  public final void testCloningGetSequence() {
    setupNodes(5);
    assertTrue(locator.getReadonlyCopy().getSequence("hi").next()
            instanceof MemcachedNodeROImpl);
  }

  protected final void assertSequence(String k, int... seq) {
    runSequenceAssertion(locator, k, seq);
    runSequenceAssertion(locator.getReadonlyCopy(), k, seq);
  }

  protected void setupNodes(int n) {
    nodes = new MemcachedNode[n];
    context = new Mockery();

    for (int i = 0; i < n; i++) {
      nodes[i] = context.mock(MemcachedNode.class, "node#" + i);
    }
  }
}
