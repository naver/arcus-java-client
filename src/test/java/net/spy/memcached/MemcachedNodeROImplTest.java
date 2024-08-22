package net.spy.memcached;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.jmock.Mockery;

import static net.spy.memcached.ExpectationsUtil.buildExpectations;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.fail;

import static org.jmock.AbstractExpectations.returnValue;

/**
 * Test readonliness of the MemcachedNodeROImpl
 */
public class MemcachedNodeROImplTest {

  private Mockery context;

  @BeforeEach
  protected void setUp() {
    context = new Mockery();
  }

  @AfterEach
  protected void tearDown() {
    context.assertIsSatisfied();
  }

  @Test
  public void testReadOnliness() throws Exception {
    SocketAddress sa = new InetSocketAddress(11211);
    MemcachedNode m = context.mock(MemcachedNode.class, "node");
    MemcachedNodeROImpl node =
            new MemcachedNodeROImpl(m);
    context.checking(buildExpectations(e -> {
      e.oneOf(m).getSocketAddress();
      e.will(returnValue(sa));
    }));

    assertSame(sa, node.getSocketAddress());
    assertEquals(m.toString(), node.toString());

    Set<String> acceptable = new HashSet<>(Arrays.asList(
            "toString", "getSocketAddress", "getBytesRemainingToWrite",
            "getReconnectCount", "getSelectionOps", "getNodeName", "hasReadOp",
            "hasWriteOp", "isActive", "isFirstConnecting"));

    for (Method meth : MemcachedNode.class.getMethods()) {
      if (acceptable.contains(meth.getName())) {
        // ok
      } else {
        Object[] args = new Object[meth.getParameterTypes().length];
        fillArgs(meth.getParameterTypes(), args);
        try {
          meth.invoke(node, args);
          fail("Failed to break on " + meth.getName());
        } catch (InvocationTargetException e) {
          assertSame(UnsupportedOperationException.class,
                  e.getCause().getClass(),
                  "Fail at " + meth.getName());
        }
      }
    }
    context.assertIsSatisfied();
  }

  private void fillArgs(Class<?>[] parameterTypes, Object[] args) {
    int i = 0;
    for (Class<?> c : parameterTypes) {
      if (c == Boolean.TYPE) {
        args[i++] = false;
      } else {
        args[i++] = null;
      }
    }
  }

}
