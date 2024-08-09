package net.spy.memcached;

import java.util.Iterator;

import junit.framework.TestCase;

import org.jmock.Mockery;

public abstract class AbstractNodeLocationCase extends TestCase {

  protected MemcachedNode[] nodes;
  protected NodeLocator locator;
  protected Mockery context;

  @Override
  protected void tearDown() throws Exception {
    super.tearDown();
    context.assertIsSatisfied();
  }

  private void runSequenceAssertion(NodeLocator l, String k, int... seq) {
    int pos = 0;
    for (Iterator<MemcachedNode> i = l.getSequence(k); i.hasNext(); ) {
      assertEquals("At position " + pos, nodes[seq[pos]].toString(),
              i.next().toString());
      try {
        i.remove();
        fail("Allowed a removal from a sequence.");
      } catch (UnsupportedOperationException e) {
        // pass
      }
      pos++;
    }
    assertEquals("Incorrect sequence size for " + k, seq.length, pos);
  }

  public final void testCloningGetPrimary() {
    setupNodes(5);
    assertTrue(locator.getReadonlyCopy().getPrimary("hi")
            instanceof MemcachedNodeROImpl);
  }

  public final void testCloningGetAll() {
    setupNodes(5);
    assertTrue(locator.getReadonlyCopy().getAll().iterator().next()
            instanceof MemcachedNodeROImpl);
  }

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
