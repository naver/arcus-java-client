package net.spy.memcached;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.junit.jupiter.api.Test;

import org.jmock.Expectations;
import org.jmock.Mockery;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Test readonliness of the MemcachedNodeROImpl
 */
public class MemcachedNodeROImplTest {

  @Test
  public void testReadOnliness() throws Exception {
    SocketAddress sa = new InetSocketAddress(11211);
    Mockery context = new Mockery();
    MemcachedNode m = context.mock(MemcachedNode.class, "node");
    MemcachedNodeROImpl node =
            new MemcachedNodeROImpl(m);
    context.checking(
            new Expectations() {{
              oneOf(m).getSocketAddress();
              will(returnValue(sa));
            }}
    );

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
