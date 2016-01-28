package net.spy.memcached;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.jmock.Mock;
import org.jmock.MockObjectTestCase;

/**
 * Test readonliness of the MemcachedNodeROImpl
 */
public class MemcachedNodeROImplTest extends MockObjectTestCase {

	public void testReadOnliness() throws Exception {
		SocketAddress sa=new InetSocketAddress(11211);
		Mock m = mock(MemcachedNode.class, "node");
		MemcachedNodeROImpl node=
			new MemcachedNodeROImpl((MemcachedNode)m.proxy());
		m.expects(once()).method("getSocketAddress").will(returnValue(sa));

		assertSame(sa, node.getSocketAddress());
		assertEquals(m.proxy().toString(), node.toString());

		/* ENABLE_REPLICATION if */
		/* WHCHOI83_MEMCACHED_REPLICA_GROUP if */
		Set<String> acceptable = new HashSet<String>(Arrays.asList(
				"toString", "getSocketAddress", "getBytesRemainingToWrite",
				"getReconnectCount", "getReconnectAccumTime", "getSelectionOps", "hasReadOp",
				"hasWriteOp", "isActive"));
		/* ENABLE_REPLICATION else */
		/* WHCHOI83_MEMCACHED_REPLICA_GROUP else */
		/*
		Set<String> acceptable = new HashSet<String>(Arrays.asList(
				"toString", "getSocketAddress", "getBytesRemainingToWrite",
				"getReconnectCount", "getSelectionOps", "hasReadOp",
				"hasWriteOp", "isActive"));
		*/
		/* WHCHOI83_MEMCACHED_REPLICA_GROUP end */
		/* ENABLE_REPLICATION end */

		for(Method meth : MemcachedNode.class.getMethods()) {
			if(acceptable.contains(meth.getName())) {
				// ok
			} else {
				Object[] args=new Object[meth.getParameterTypes().length];
				fillArgs(meth.getParameterTypes(), args);
				try {
					meth.invoke(node, args);
					fail("Failed to break on " + meth.getName());
				} catch(InvocationTargetException e) {
					assertSame("Fail at " + meth.getName(),
						UnsupportedOperationException.class,
						e.getCause().getClass());
				}
			}
		}
	}

	private void fillArgs(Class<?>[] parameterTypes, Object[] args) {
		int i=0;
		for(Class<?> c : parameterTypes) {
			/* ENABLE_REPLICATION if */
			/* WHCHOI83_MEMCACHED_REPLICA_GROUP if */
			if(c == Boolean.TYPE) {
				args[i++] = false;
			} else if (c == Long.TYPE) {
				args[i++] = 0;
			} else {
				args[i++] = null;
			}
			/* ENABLE_REPLICATION else */
			/* WHCHOI83_MEMCACHED_REPLICA_GROUP else */
			/*
			if(c == Boolean.TYPE) {
				args[i++] = false;
			} else {
				args[i++] = null;
			}
			*/
			/* WHCHOI83_MEMCACHED_REPLICA_GROUP end */
			/* ENABLE_REPLICATION end */
		}
	}

}
