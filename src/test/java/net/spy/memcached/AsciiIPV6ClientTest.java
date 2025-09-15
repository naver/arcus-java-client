package net.spy.memcached;

import java.util.Collections;

/**
 * Test the test protocol over IPv6.
 */
class AsciiIPV6ClientTest extends AsciiClientTest {

  @Override
  protected void initClient(ConnectionFactoryBuilder cfb) throws Exception {
    client = new MemcachedClient(cfb.build(),
            AddrUtil.getAddresses(Collections.singletonList("::1:11211")));
  }

  @Override
  protected String getExpectedVersionSource() {
    return "/0:0:0:0:0:0:0:1:11211";
  }

}
