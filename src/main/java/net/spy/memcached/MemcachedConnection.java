/*
 * arcus-java-client : Arcus Java client
 * Copyright 2010-2014 NAVER Corp.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
// Copyright (c) 2006  Dustin Sallings <dustin@spy.net>

package net.spy.memcached;

import java.io.IOException;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.SocketException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;

import net.spy.memcached.compat.SpyObject;
import net.spy.memcached.compat.log.LoggerFactory;
import net.spy.memcached.ops.KeyedOperation;
import net.spy.memcached.ops.Operation;
import net.spy.memcached.ops.OperationException;
import net.spy.memcached.ops.OperationState;

/**
 * Connection to a cluster of memcached servers.
 */
public final class MemcachedConnection extends SpyObject {

	// The number of empty selects we'll allow before assuming we may have
	// missed one and should check the current selectors.  This generally
	// indicates a bug, but we'll check it nonetheless.
	private static final int DOUBLE_CHECK_EMPTY = 256;
	// The number of empty selects we'll allow before blowing up.  It's too
	// easy to write a bug that causes it to loop uncontrollably.  This helps
	// find those bugs and often works around them.
	private static final int EXCESSIVE_EMPTY = 0x1000000;

	private volatile boolean shutDown=false;
	// If true, optimization will collapse multiple sequential get ops
	private final boolean shouldOptimize;
	private Selector selector=null;
	private final NodeLocator locator;
	private final FailureMode failureMode;
	// maximum amount of time to wait between reconnect attempts
	private final long maxDelay;
	private int emptySelects=0;
	// AddedQueue is used to track the QueueAttachments for which operations
	// have recently been queued.
	private final ConcurrentLinkedQueue<MemcachedNode> addedQueue;
	// reconnectQueue contains the attachments that need to be reconnected
	// The key is the time at which they are eligible for reconnect
	private final SortedMap<Long, MemcachedNode> reconnectQueue;
	private final Collection<ConnectionObserver> connObservers =
		new ConcurrentLinkedQueue<ConnectionObserver>();
	private final OperationFactory opFact;
	private final int timeoutExceptionThreshold;

	private BlockingQueue<String> _nodeManageQueue = new LinkedBlockingQueue<String>();
	private final ConnectionFactory f;

	/* ENABLE_REPLICATION if */
	private boolean arcusReplEnabled;
	/* ENABLE_REPLICATION end */

	/**
	 * Construct a memcached connection.
	 *
	 * @param bufSize the size of the buffer used for reading from the server
	 * @param f the factory that will provide an operation queue
	 * @param a the addresses of the servers to connect to
	 *
	 * @throws IOException if a connection attempt fails early
	 */
	public MemcachedConnection(int bufSize, ConnectionFactory f,
			List<InetSocketAddress> a, Collection<ConnectionObserver> obs,
			FailureMode fm, OperationFactory opfactory)
		throws IOException {
		this.f = f;
		connObservers.addAll(obs);
		reconnectQueue=new TreeMap<Long, MemcachedNode>();
		addedQueue=new ConcurrentLinkedQueue<MemcachedNode>();
		failureMode = fm;
		shouldOptimize = f.shouldOptimize();
		maxDelay = f.getMaxReconnectDelay();
		opFact = opfactory;
		timeoutExceptionThreshold = f.getTimeoutExceptionThreshold();
		selector=Selector.open();
		List<MemcachedNode> connections=new ArrayList<MemcachedNode>(a.size());
		for(SocketAddress sa : a) {
			connections.add(attachMemcachedNode(sa));
		}
		locator=f.createLocator(connections);
	}

	/* ENABLE_REPLICATION if */
	// handleNodeManageQueue and updateConnections behave slightly differently
	// depending on the Arcus version.  We could have created a subclass and overload
	// those methods.  But, MemcachedConnection is a final class.
	void setArcusReplEnabled(boolean b) {
		arcusReplEnabled = b;
	}
	/* ENABLE_REPLICATION end */

	private boolean selectorsMakeSense() {
		for(MemcachedNode qa : locator.getAll()) {
			if(qa.getSk() != null && qa.getSk().isValid()) {
				if(qa.getChannel().isConnected()) {
					int sops=qa.getSk().interestOps();
					int expected=0;
					if(qa.hasReadOp()) {
						expected |= SelectionKey.OP_READ;
					}
					if(qa.hasWriteOp()) {
						expected |= SelectionKey.OP_WRITE;
					}
					if(qa.getBytesRemainingToWrite() > 0) {
						expected |= SelectionKey.OP_WRITE;
					}
					assert sops == expected : "Invalid ops:  "
						+ qa + ", expected " + expected + ", got " + sops;
				} else {
					int sops=qa.getSk().interestOps();
					assert sops == SelectionKey.OP_CONNECT
					: "Not connected, and not watching for connect: "
						+ sops;
				}
			}
		}
		getLogger().debug("Checked the selectors.");
		return true;
	}

	/**
	 * MemcachedClient calls this method to handle IO over the connections.
	 */
	public void handleIO() throws IOException {
		if(shutDown) {
			throw new IOException("No IO while shut down");
		}

		// Deal with all of the stuff that's been added, but may not be marked
		// writable.
		handleInputQueue();
		getLogger().debug("Done dealing with queue.");

		long delay=0;
		if(!reconnectQueue.isEmpty()) {
			long now=System.currentTimeMillis();
			long then=reconnectQueue.firstKey();
			delay=Math.max(then-now, 1);
		}
		getLogger().debug("Selecting with delay of %sms", delay);
		assert selectorsMakeSense() : "Selectors don't make sense.";
		int selected=selector.select(delay);
		Set<SelectionKey> selectedKeys=selector.selectedKeys();

		if(selectedKeys.isEmpty() && !shutDown) {
			getLogger().debug("No selectors ready, interrupted: "
					+ Thread.interrupted());
			if(++emptySelects > DOUBLE_CHECK_EMPTY) {
				for(SelectionKey sk : selector.keys()) {
					getLogger().info("%s has %s, interested in %s",
							sk, sk.readyOps(), sk.interestOps());
					if(sk.readyOps() != 0) {
						getLogger().info("%s has a ready op, handling IO", sk);
						handleIO(sk);
					} else {
						lostConnection((MemcachedNode)sk.attachment());
					}
				}
				assert emptySelects < EXCESSIVE_EMPTY
					: "Too many empty selects";
			}
		} else {
			getLogger().debug("Selected %d, selected %d keys",
					selected, selectedKeys.size());
			emptySelects=0;

			for(SelectionKey sk : selectedKeys) {
				handleIO(sk);
			}

			selectedKeys.clear();
		}

		// see if any connections blew up with large number of timeouts
		for(SelectionKey sk : selector.keys()) {
			MemcachedNode mn = (MemcachedNode)sk.attachment();
			if (mn.getContinuousTimeout() > timeoutExceptionThreshold)
			{
				getLogger().warn(
						"%s exceeded continuous timeout threshold. >%s (%s)",
						mn.getSocketAddress().toString(), timeoutExceptionThreshold, mn.getStatus());
				lostConnection(mn);
			}
		}

		// Deal with the memcached server group that's been added by CacheManager.  
		handleNodeManageQueue();
		
		if(!shutDown && !reconnectQueue.isEmpty()) {
			attemptReconnects();
		}
	}
	
	public void updateConnections(List<InetSocketAddress> addrs) throws IOException {
		List<MemcachedNode> attachNodes = new ArrayList<MemcachedNode>();
		List<MemcachedNode> removeNodes = new ArrayList<MemcachedNode>();
		
		// Classify the incoming node list.
		/* ENABLE_REPLICATION if */
		if (arcusReplEnabled) {
			// If there's an existing node with the same group name as the new node,
			// remove it.  And add the new node.
			for (MemcachedNode node : locator.getAll()) {
				ArcusReplNodeAddress oldNode = (ArcusReplNodeAddress)node.getSocketAddress();
				Iterator<InetSocketAddress> itr = addrs.iterator();
				while (itr.hasNext()) {
					ArcusReplNodeAddress newNode = (ArcusReplNodeAddress)itr.next();
					if (oldNode.group.equals(newNode.group)) {
						if (oldNode.master == newNode.master &&
						    oldNode.ip.equals(newNode.ip) &&
						    oldNode.port == newNode.port) {
							// The exactly same node appears in the new list.
							// Leave it alone.
							itr.remove();
							node = null;
						} else {
							// The new list has a node with the same group name
							// as the old node.  But they are not the same.
							// Remove the old node and add the new node.
						}
						break;
					}
				}
				if (node != null) {
					removeNodes.add(node);
				}
			}
		} else {
			for (MemcachedNode node : locator.getAll()) {
				if (addrs.contains((InetSocketAddress) node.getSocketAddress())) {
					addrs.remove((InetSocketAddress) node.getSocketAddress());
				} else {
					removeNodes.add(node);
				}
			}
		}
		/* ENABLE_REPLICATION else */
		/*
		for (MemcachedNode node : locator.getAll()) {
			if (addrs.contains((InetSocketAddress) node.getSocketAddress())) {
				addrs.remove((InetSocketAddress) node.getSocketAddress());
			} else {
				removeNodes.add(node);
			}
		}
		*/
		/* ENABLE_REPLICATION end */
		
		// Make connections to the newly added nodes.
		for (SocketAddress sa : addrs) {
			attachNodes.add(attachMemcachedNode(sa));
		}

		// Remove unavailable nodes in the reconnect queue.
		for (MemcachedNode node : removeNodes) {
			getLogger().info("old memcached node removed %s", node);
			for (Entry<Long, MemcachedNode> each : reconnectQueue.entrySet()) {
				if (node.equals(each.getValue())) {
					reconnectQueue.remove(each.getKey());
					break;
				}
			}
		}
		
		// Update the hash.
		locator.update(attachNodes, removeNodes);
	}
	
	MemcachedNode attachMemcachedNode(SocketAddress sa) throws IOException {
		SocketChannel ch = SocketChannel.open();
		ch.configureBlocking(false);
		// bufSize : 16384 (default value)
		MemcachedNode qa = 
				f.createMemcachedNode(sa, ch, f.getReadBufSize());
		int ops = 0;
		ch.socket().setTcpNoDelay(!f.useNagleAlgorithm());
		ch.socket().setReuseAddress(true);
		/* ENABLE_REPLICATION if */
		// Do not attempt to connect if this node is fake.
		// Otherwise, we keep connecting to a non-existent listen address
		// and keep failing/reconnecting.
		if (qa.isFake()) {
			// Locator assumes non-null selectionkey.  So add a dummy one...
			qa.setSk(ch.register(selector, ops, qa));
			return qa;
		}
		/* ENABLE_REPLICATION end */
		// Initially I had attempted to skirt this by queueing every
		// connect, but it considerably slowed down start time.
		try {
			if (ch.connect(sa)) {
				getLogger().info("new memcached node connected to %s immediately", qa);
				// FIXME.  Do we ever execute this path?
				// This method does not call observer.connectionEstablished.
				qa.connected();
			} else {
				getLogger().info("new memcached node added %s to connect queue", qa);
				ops = SelectionKey.OP_CONNECT;
			}
			qa.setSk(ch.register(selector, ops, qa));
			assert ch.isConnected()
					|| qa.getSk().interestOps() == SelectionKey.OP_CONNECT 
					: "Not connected, and not wanting to connect";
		} catch (SocketException e) {
			getLogger().warn("new memcached socket error on initial connect");
			queueReconnect(qa);
		}
		return qa;
	}
	
	public void putMemcachedQueue(String addrs) {
		_nodeManageQueue.offer(addrs);
	}	

	// Handle the memcached server group that's been added by CacheManager.
	void handleNodeManageQueue() throws IOException {
		if (_nodeManageQueue.isEmpty()) {
			return;
		}
		
		// Get addresses from the queue
		String addrs = _nodeManageQueue.poll();
		
		// Update the memcached server group.
		/* ENABLE_REPLICATION if */
		if (arcusReplEnabled)
			updateConnections(ArcusReplNodeAddress.getAddresses(addrs));
		else
			updateConnections(AddrUtil.getAddresses(addrs));
		/* ENABLE_REPLICATION else */
		/*
		updateConnections(AddrUtil.getAddresses(addrs));
		*/
		/* ENABLE_REPLICATION end */
	}	
	
	// Handle any requests that have been made against the client.
	private void handleInputQueue() {
		if(!addedQueue.isEmpty()) {
			getLogger().debug("Handling queue");
			// If there's stuff in the added queue.  Try to process it.
			Collection<MemcachedNode> toAdd=new HashSet<MemcachedNode>();
			// Transfer the queue into a hashset.  There are very likely more
			// additions than there are nodes.
			Collection<MemcachedNode> todo=new HashSet<MemcachedNode>();

			MemcachedNode node;
			while ((node = addedQueue.poll()) != null) {
				todo.add(node);
			}

			// Now process the queue.
			for(MemcachedNode qa : todo) {
				boolean readyForIO=false;
				if(qa.isActive()) {
					if(qa.getCurrentWriteOp() != null) {
						readyForIO=true;
						getLogger().debug("Handling queued write %s", qa);
					}
				} else {
					toAdd.add(qa);
				}
				qa.copyInputQueue();
				if(readyForIO) {
					try {
						if(qa.getWbuf().hasRemaining()) {
							handleWrites(qa.getSk(), qa);
						}
					} catch(IOException e) {
						getLogger().warn("Exception handling write", e);
						lostConnection(qa);
					}
				}
				qa.fixupOps();
			}
			addedQueue.addAll(toAdd);
		}
	}

	/**
	 * Add a connection observer.
	 *
	 * @return whether the observer was successfully added
	 */
	public boolean addObserver(ConnectionObserver obs) {
		return connObservers.add(obs);
	}

	/**
	 * Remove a connection observer.
	 *
	 * @return true if the observer existed and now doesn't
	 */
	public boolean removeObserver(ConnectionObserver obs) {
		return connObservers.remove(obs);
	}

	private void connected(MemcachedNode qa) {
		assert qa.getChannel().isConnected() : "Not connected.";
		int rt = qa.getReconnectCount();
		qa.connected();
		for(ConnectionObserver observer : connObservers) {
			observer.connectionEstablished(qa.getSocketAddress(), rt);
		}
	}

	private void lostConnection(MemcachedNode qa) {
		queueReconnect(qa);
		for(ConnectionObserver observer : connObservers) {
			observer.connectionLost(qa.getSocketAddress());
		}
	}

	// Handle IO for a specific selector.  Any IOException will cause a
	// reconnect
	private void handleIO(SelectionKey sk) {
		MemcachedNode qa=(MemcachedNode)sk.attachment();
		try {
			getLogger().debug(
					"Handling IO for:  %s (r=%s, w=%s, c=%s, op=%s)",
					sk, sk.isReadable(), sk.isWritable(),
					sk.isConnectable(), sk.attachment());
			if(sk.isConnectable()) {
				getLogger().info("Connection state changed for %s", sk);
				final SocketChannel channel=qa.getChannel();
				if(channel.finishConnect()) {
					connected(qa);
					addedQueue.offer(qa);
					if(qa.getWbuf().hasRemaining()) {
						handleWrites(sk, qa);
					}
				} else {
					assert !channel.isConnected() : "connected";
				}
			} else {
				if(sk.isValid() && sk.isReadable()) {
					handleReads(sk, qa);
				}
				if(sk.isValid() && sk.isWritable()) {
					handleWrites(sk, qa);
				}
			}
		} catch(ClosedChannelException e) {
			// Note, not all channel closes end up here
			if(!shutDown) {
				getLogger().info("Closed channel and not shutting down.  "
					+ "Queueing reconnect on %s", qa, e);
				lostConnection(qa);
			}
		} catch(ConnectException e) {
			// Failures to establish a connection should attempt a reconnect
			// without signaling the observers.
			getLogger().info("Reconnecting due to failure to connect to %s",
					qa, e);
			queueReconnect(qa);
		} catch (OperationException e) {
			qa.setupForAuth(); // noop if !shouldAuth
			getLogger().info("Reconnection due to exception " +
				"handling a memcached operation on %s.  " +
				"This may be due to an authentication failure.", qa, e);
			lostConnection(qa);
		} catch(Exception e) {
			// Any particular error processing an item should simply
			// cause us to reconnect to the server.
			//
			// One cause is just network oddness or servers
			// restarting, which lead here with IOException

			qa.setupForAuth(); // noop if !shouldAuth
			getLogger().info("Reconnecting due to exception on %s", qa, e);
			lostConnection(qa);
		}
		qa.fixupOps();
	}

	private void handleWrites(SelectionKey sk, MemcachedNode qa)
		throws IOException {
		qa.fillWriteBuffer(shouldOptimize);
		boolean canWriteMore=qa.getBytesRemainingToWrite() > 0;
		while(canWriteMore) {
			int wrote=qa.writeSome();
			qa.fillWriteBuffer(shouldOptimize);
			canWriteMore = wrote > 0 && qa.getBytesRemainingToWrite() > 0;
		}
	}

	private void handleReads(SelectionKey sk, MemcachedNode qa)
		throws IOException {
		Operation currentOp = qa.getCurrentReadOp();
		ByteBuffer rbuf=qa.getRbuf();
		final SocketChannel channel = qa.getChannel();
		int read=channel.read(rbuf);
		if (read < 0) {
		    // our model is to keep the connection alive for future ops
		    // so we'll queue a reconnect if disconnected via an IOException
		    throw new IOException("Disconnected unexpected, will reconnect.");
		}
		while(read > 0) {
			getLogger().debug("Read %d bytes", read);
			rbuf.flip();
			while(rbuf.remaining() > 0) {
				if(currentOp == null) {
					throw new IllegalStateException("No read operation.");
				}
				currentOp.readFromBuffer(rbuf);
				if(currentOp.getState() == OperationState.COMPLETE) {
					getLogger().debug(
							"Completed read op: %s and giving the next %d bytes",
							currentOp, rbuf.remaining());
					Operation op=qa.removeCurrentReadOp();
					assert op == currentOp
					: "Expected to pop " + currentOp + " got " + op;
					currentOp=qa.getCurrentReadOp();
				}
			}
			rbuf.clear();
			read=channel.read(rbuf);
		}
	}

	// Make a debug string out of the given buffer's values
	static String dbgBuffer(ByteBuffer b, int size) {
		StringBuilder sb=new StringBuilder();
		byte[] bytes=b.array();
		for(int i=0; i<size; i++) {
			char ch=(char)bytes[i];
			if(Character.isWhitespace(ch) || Character.isLetterOrDigit(ch)) {
				sb.append(ch);
			} else {
				sb.append("\\x");
				sb.append(Integer.toHexString(bytes[i] & 0xff));
			}
		}
		return sb.toString();
	}

	private void queueReconnect(MemcachedNode qa) {
		if(!shutDown) {
			getLogger().warn("Closing, and reopening %s, attempt %d.", qa,
					qa.getReconnectCount());
			if(qa.getSk() != null) {
				qa.getSk().cancel();
				assert !qa.getSk().isValid() : "Cancelled selection key is valid";
			}
			qa.reconnecting();
			try {
				if(qa.getChannel() != null && qa.getChannel().socket() != null) {
					qa.getChannel().socket().close();
				} else {
					getLogger().info("The channel or socket was null for %s",
						qa);
				}
			} catch(IOException e) {
				getLogger().warn("IOException trying to close a socket", e);
			}
			qa.setChannel(null);

			long delay = (long)Math.min(maxDelay,
					Math.pow(2, qa.getReconnectCount())) * 1000;
			long reconTime = System.currentTimeMillis() + delay;

			// Avoid potential condition where two connections are scheduled
			// for reconnect at the exact same time.  This is expected to be
			// a rare situation.
			while(reconnectQueue.containsKey(reconTime)) {
				reconTime++;
			}

			reconnectQueue.put(reconTime, qa);

			// Need to do a little queue management.
			qa.setupResend();

			if(failureMode == FailureMode.Redistribute) {
				redistributeOperations(qa.destroyInputQueue());
			} else if(failureMode == FailureMode.Cancel) {
				cancelOperations(qa.destroyInputQueue());
			}
		}
	}

	private void cancelOperations(Collection<Operation> ops) {
		for(Operation op : ops) {
			op.cancel();
		}
	}

	private void redistributeOperations(Collection<Operation> ops) {
		for(Operation op : ops) {
			if(op instanceof KeyedOperation) {
				KeyedOperation ko = (KeyedOperation)op;
				int added = 0;
				for(String k : ko.getKeys()) {
					for(Operation newop : opFact.clone(ko)) {
						addOperation(k, newop);
						added++;
					}
				}
				assert added > 0
					: "Didn't add any new operations when redistributing";
			} else {
				// Cancel things that don't have definite targets.
				op.cancel();
			}
		}
	}

	private void attemptReconnects() throws IOException {
		final long now=System.currentTimeMillis();
		final Map<MemcachedNode, Boolean> seen=
			new IdentityHashMap<MemcachedNode, Boolean>();
		final List<MemcachedNode> rereQueue=new ArrayList<MemcachedNode>();
		SocketChannel ch = null;
		for(Iterator<MemcachedNode> i=
				reconnectQueue.headMap(now).values().iterator(); i.hasNext();) {
			final MemcachedNode qa=i.next();
			i.remove();
			try {
				if(!seen.containsKey(qa)) {
					seen.put(qa, Boolean.TRUE);
					getLogger().info("Reconnecting %s", qa);
					ch=SocketChannel.open();
					ch.configureBlocking(false);
					int ops=0;
					if(ch.connect(qa.getSocketAddress())) {
						getLogger().info("Immediately reconnected to %s", qa);
						assert ch.isConnected();
					} else {
						ops=SelectionKey.OP_CONNECT;
					}
					qa.registerChannel(ch, ch.register(selector, ops, qa));
					assert qa.getChannel() == ch : "Channel was lost.";
				} else {
					getLogger().debug(
						"Skipping duplicate reconnect request for %s", qa);
				}
			} catch(SocketException e) {
				getLogger().warn("Error on reconnect", e);
				rereQueue.add(qa);
			}
			catch (Exception e) {
                getLogger().error("Exception on reconnect, lost node %s", qa, e);
            } finally {
                //it's possible that above code will leak file descriptors under abnormal
                //conditions (when ch.open() fails and throws IOException.
                //always close non connected channel
                if (ch != null && !ch.isConnected()
                        && !ch.isConnectionPending()) {
                    try {
                        ch.close();
                    } catch (IOException x) {
                        getLogger().error("Exception closing channel: %s", qa, x);
                    }
                }
            }
		}
		// Requeue any fast-failed connects.
		for(MemcachedNode n : rereQueue) {
			queueReconnect(n);
		}
	}

	/**
	 * Get the node locator used by this connection.
	 */
	NodeLocator getLocator() {
		return locator;
	}
	
	Selector getSelector() {
		return selector;
	}

	/**
	 * Add an operation to the given connection.
	 *
	 * @param key the key the operation is operating upon
	 * @param o the operation
	 */
	public void addOperation(final String key, final Operation o) {
		MemcachedNode placeIn=null;
		MemcachedNode primary = locator.getPrimary(key);
		if(primary.isActive() || failureMode == FailureMode.Retry) {
			placeIn=primary;
		} else if(failureMode == FailureMode.Cancel) {
			o.setHandlingNode(primary);
			o.cancel();
		} else {
			// Look for another node in sequence that is ready.
			for(Iterator<MemcachedNode> i=locator.getSequence(key);
				placeIn == null && i.hasNext(); ) {
				MemcachedNode n=i.next();
				if(n.isActive()) {
					placeIn=n;
				}
			}
			// If we didn't find an active node, queue it in the primary node
			// and wait for it to come back online.
			if(placeIn == null) {
				placeIn = primary;
			}
		}

		assert o.isCancelled() || placeIn != null
			: "No node found for key " + key;
		if(placeIn != null) {
			addOperation(placeIn, o);
		} else {
			assert o.isCancelled() : "No not found for "
				+ key + " (and not immediately cancelled)";
		}
	}

	public void insertOperation(final MemcachedNode node, final Operation o) {
		o.setHandlingNode(node);
		o.initialize();
		node.insertOp(o);
		addedQueue.offer(node);
		Selector s=selector.wakeup();
		assert s == selector : "Wakeup returned the wrong selector.";
		getLogger().debug("Added %s to %s", o, node);
	}

	public void addOperation(final MemcachedNode node, final Operation o) {
		o.setHandlingNode(node);
		o.initialize();
		node.addOp(o);
		addedQueue.offer(node);
		Selector s=selector.wakeup();
		assert s == selector : "Wakeup returned the wrong selector.";
		getLogger().debug("Added %s to %s", o, node);
	}

	public void addOperations(final Map<MemcachedNode, Operation> ops) {

		for(Map.Entry<MemcachedNode, Operation> me : ops.entrySet()) {
			final MemcachedNode node=me.getKey();
			Operation o=me.getValue();
			o.setHandlingNode(node);
			o.initialize();
			node.addOp(o);
			addedQueue.offer(node);
		}
		Selector s=selector.wakeup();
		assert s == selector : "Wakeup returned the wrong selector.";
	}

	/**
	 * Broadcast an operation to all nodes.
	 */
	public CountDownLatch broadcastOperation(BroadcastOpFactory of) {
		return broadcastOperation(of, locator.getAll());
	}

	/**
	 * Broadcast an operation to a specific collection of nodes.
	 */
	public CountDownLatch broadcastOperation(final BroadcastOpFactory of,
			Collection<MemcachedNode> nodes) {
		final CountDownLatch latch=new CountDownLatch(locator.getAll().size());
		for(MemcachedNode node : nodes) {
			Operation op = of.newOp(node, latch);
			op.initialize();
			node.addOp(op);
			op.setHandlingNode(node);
			addedQueue.offer(node);
		}
		Selector s=selector.wakeup();
		assert s == selector : "Wakeup returned the wrong selector.";
		return latch;
	}

	/**
	 * Shut down all of the connections.
	 */
	public void shutdown() throws IOException {
		shutDown=true;
		Selector s=selector.wakeup();
		assert s == selector : "Wakeup returned the wrong selector.";
		for(MemcachedNode qa : locator.getAll()) {
			qa.shutdown();
		}
		selector.close();
		getLogger().debug("Shut down selector %s", selector);
	}

	@Override
	public String toString() {
		StringBuilder sb=new StringBuilder();
		sb.append("{MemcachedConnection to");
		for(MemcachedNode qa : locator.getAll()) {
			sb.append(" ");
			sb.append(qa.getSocketAddress());
		}
		sb.append("}");
		return sb.toString();
	}

	/**
     * helper method: increase timeout count on node attached to this op
     *
     * @param op
     */
    public static void opTimedOut(Operation op) {
        MemcachedConnection.setTimeout(op, true);
    }

    /**
     * helper method: reset timeout counter
     *
     * @param op
     */
    public static void opSucceeded(Operation op) {
        MemcachedConnection.setTimeout(op, false);
    }

    /**
     * helper method: do some error checking and set timeout boolean
     *
     * @param op
     * @param isTimeout
     */
    private static void setTimeout(Operation op, boolean isTimeout) {
        try {
			if (op == null) {
				LoggerFactory.getLogger(MemcachedConnection.class).debug("op is null.");
				return; // op may be null in some cases, e.g. flush
			}
            MemcachedNode node = op.getHandlingNode();
            if (node == null) {
            	LoggerFactory.getLogger(MemcachedConnection.class).debug("handling node for operation is not set");
            }
            else {
                node.setContinuousTimeout(isTimeout);
            }
        } catch (Exception e) {
            LoggerFactory.getLogger(MemcachedConnection.class).error(e.getMessage());
        }
    }

    /**
     * find memcachednode for key
     * @param key
     * @return a memcached node
     */
	public MemcachedNode findNodeByKey(String key) {
		MemcachedNode placeIn = null;
		MemcachedNode primary = locator.getPrimary(key);
		// FIXME.  Support other FailureMode's.  See MemcachedConnection.addOperation.
		if (primary.isActive() || failureMode == FailureMode.Retry) {
			placeIn = primary;
		} else {
			for (Iterator<MemcachedNode> i = locator.getSequence(key); placeIn == null
					&& i.hasNext();) {
				MemcachedNode n = i.next();
				if (n.isActive()) {
					placeIn = n;
				}
			}
			if (placeIn == null) {
				placeIn = primary;
			}
		}
		return placeIn;
	}
	
	public int getAddedQueueSize() {
		return addedQueue.size();
	}
}
