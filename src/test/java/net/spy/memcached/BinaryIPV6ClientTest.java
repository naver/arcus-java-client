package net.spy.memcached;

import java.util.Collections;

/**
 * Binary IPv6 client test.
 */
class BinaryIPV6ClientTest extends BinaryClientTest {

  @Override
  protected void initClient(ConnectionFactory cf) throws Exception {
    client = new MemcachedClient(cf, AddrUtil.getAddresses(
            Collections.singletonList("::1:11211")));
  }

  @Override
  protected String getExpectedVersionSource() {
    return "/0:0:0:0:0:0:0:1:11211";
  }

}
