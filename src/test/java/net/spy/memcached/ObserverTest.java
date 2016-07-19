package net.spy.memcached;

import java.net.SocketAddress;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import net.spy.memcached.compat.SpyObject;

/**
 * Test observer hooks.
 */
public class ObserverTest extends ClientBaseCase {

	public void testConnectionObserver() throws Exception {
		ConnectionObserver obs = new LoggingObserver();
		assertTrue("Didn't add observer.", client.addObserver(obs));
		assertTrue("Didn't remove observer.", client.removeObserver(obs));
		assertFalse("Removed observer more than once.",
			client.removeObserver(obs));
	}

	static class LoggingObserver extends SpyObject
		implements ConnectionObserver {
		public void connectionEstablished(SocketAddress sa,
				int reconnectCount) {
			getLogger().info("Connection established to %s (%s)",
				sa, reconnectCount);
		}

		public void connectionLost(SocketAddress sa) {
			getLogger().info("Connection lost from %s", sa);
		}

	}
}
