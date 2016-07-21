package net.spy.memcached;

import java.net.SocketAddress;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import net.spy.memcached.compat.SpyObject;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.internal.runners.JUnit4ClassRunner;
import org.junit.runner.RunWith;

import static org.junit.Assume.assumeTrue;

/**
 * Test observer hooks.
 */
@RunWith(JUnit4ClassRunner.class)
public class ObserverTest extends ClientBaseCase {

	@Before
	public void setup() throws Exception {
		super.setUp();
	}

	@Test
	public void testConnectionObserver() throws Exception {
		ConnectionObserver obs = new LoggingObserver();
		assertTrue("Didn't add observer.", client.addObserver(obs));
		assertTrue("Didn't remove observer.", client.removeObserver(obs));
		assertFalse("Removed observer more than once.",
			client.removeObserver(obs));
	}

	@Test
	public void testInitialObservers() throws Exception {
		assumeTrue(!USE_ZK);
		assertTrue("Couldn't shut down within five seconds",
						client.shutdown(5, TimeUnit.SECONDS));

		final CountDownLatch latch = new CountDownLatch(1);
		final ConnectionObserver obs = new ConnectionObserver() {

			public void connectionEstablished(SocketAddress sa,
																				int reconnectCount) {
				latch.countDown();
			}

			public void connectionLost(SocketAddress sa) {
				assert false : "Should not see this.";
			}

		};

		// Get a new client
		initClient(new DefaultConnectionFactory() {
			@Override
			public Collection<ConnectionObserver> getInitialObservers() {
				return Collections.singleton(obs);
			}
		});

		assertTrue("Didn't detect connection",
						latch.await(2, TimeUnit.SECONDS));
		assertTrue("Did not install observer.", client.removeObserver(obs));
		assertFalse("Didn't clean up observer.", client.removeObserver(obs));
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

	@After
	public void tearDown() throws Exception {
		super.tearDown();
	}
}
