package net.spy.memcached;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import junit.framework.TestCase;

/**
 * Test the various memcached client constructors.
 */
public class MemcachedClientConstructorTest extends TestCase {

  private MemcachedClient client = null;

  protected static String ARCUS_HOST = System
          .getProperty("ARCUS_HOST",
                  "127.0.0.1:11211");

  protected static boolean USE_ZK = Boolean.valueOf(System.getProperty(
          "USE_ZK", "false"));

  @Override
  protected void tearDown() throws Exception {
    if (client != null) {
      client.shutdown();
    }
    super.tearDown();
  }

  private void assertWorking() throws Exception {
    Map<SocketAddress, String> versions = client.getVersions();
    assertEquals("/" + ARCUS_HOST,
            versions.keySet().iterator().next().toString());
  }

  private void assertArgRequired(IllegalArgumentException e) {
    assertEquals("You must have at least one server to connect to",
            e.getMessage());
  }

  public void testVarargConstructor() throws Exception {
    if (!USE_ZK) {
      String tokens[] = ARCUS_HOST.split(":");
      String ip = tokens[0];
      int port = Integer.valueOf(tokens[1]);
      client = new MemcachedClient(
              new InetSocketAddress(InetAddress.getByName(ip), port));
      assertWorking();
    }
  }

  public void testEmptyVarargConstructor() throws Exception {
    try {
      client = new MemcachedClient();
      fail("Expected illegal arg exception, got " + client);
    } catch (IllegalArgumentException e) {
      assertArgRequired(e);
    }
  }

  public void testNulListConstructor() throws Exception {
    try {
      List<InetSocketAddress> l = null;
      client = new MemcachedClient(l);
      fail("Expected null pointer exception, got " + client);
    } catch (NullPointerException e) {
      assertEquals("Server list required", e.getMessage());
    }
  }

  public void testEmptyListConstructor() throws Exception {
    try {
      client = new MemcachedClient(
              Collections.<InetSocketAddress>emptyList());
      fail("Expected illegal arg exception, got " + client);
    } catch (IllegalArgumentException e) {
      assertArgRequired(e);
    }
  }

  public void testNullFactoryConstructor() throws Exception {
    try {
      client = new MemcachedClient(null,
              AddrUtil.getAddresses(ARCUS_HOST));
      fail("Expected null pointer exception, got " + client);
    } catch (NullPointerException e) {
      assertEquals("Connection factory required", e.getMessage());
    }
  }

  public void testNegativeTimeout() throws Exception {
    try {
      client = new MemcachedClient(new DefaultConnectionFactory() {
        @Override
        public long getOperationTimeout() {
          return -1;
        }
      },
              AddrUtil.getAddresses(ARCUS_HOST));
      fail("Expected null pointer exception, got " + client);
    } catch (IllegalArgumentException e) {
      assertEquals("Operation timeout must be positive.", e.getMessage());
    }
  }

  public void testZeroTimeout() throws Exception {
    try {
      client = new MemcachedClient(new DefaultConnectionFactory() {
        @Override
        public long getOperationTimeout() {
          return 0;
        }
      },
              AddrUtil.getAddresses(ARCUS_HOST));
      fail("Expected null pointer exception, got " + client);
    } catch (IllegalArgumentException e) {
      assertEquals("Operation timeout must be positive.", e.getMessage());
    }
  }

  public void testConnFactoryWithoutOpFactory() throws Exception {
    try {
      client = new MemcachedClient(new DefaultConnectionFactory() {
        @Override
        public OperationFactory getOperationFactory() {
          return null;
        }
      }, AddrUtil.getAddresses(ARCUS_HOST));
      fail("Expected AssertionError, got " + client);
    } catch (AssertionError e) {
      assertEquals("Connection factory failed to make op factory",
              e.getMessage());
    }
  }

  public void testConnFactoryWithoutConns() throws Exception {
    try {
      client = new MemcachedClient(new DefaultConnectionFactory() {
        @Override
        public MemcachedConnection createConnection(String name,
                List<InetSocketAddress> addrs) throws IOException {
          return null;
        }
      }, AddrUtil.getAddresses(ARCUS_HOST));
      fail("Expected AssertionError, got " + client);
    } catch (AssertionError e) {
      assertEquals("Connection factory failed to make a connection",
              e.getMessage());
    }

  }

  public void testArraymodNodeLocatorAccessor() throws Exception {
    client = new MemcachedClient(AddrUtil.getAddresses(ARCUS_HOST));
    assertTrue(client.getNodeLocator() instanceof ArrayModNodeLocator);
    assertTrue(client.getNodeLocator().getPrimary("x")
            instanceof MemcachedNodeROImpl);
  }

  public void testKetamaNodeLocatorAccessor() throws Exception {
    client = new MemcachedClient(new KetamaConnectionFactory(),
            AddrUtil.getAddresses(ARCUS_HOST));
    assertTrue(client.getNodeLocator() instanceof KetamaNodeLocator);
    assertTrue(client.getNodeLocator().getPrimary("x")
            instanceof MemcachedNodeROImpl);
  }

}
