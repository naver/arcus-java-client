/*
 * arcus-java-client : Arcus Java client
 * Copyright 2010-2014 NAVER Corp.
 * Copyright 2014-2022 JaM2in Co., Ltd.
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
package net.spy.memcached.protocol;

import java.io.IOException;
import java.net.SocketAddress;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.Collection;
import java.util.StringTokenizer;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import net.spy.memcached.AddrUtil;
import net.spy.memcached.ArcusReplNodeAddress;
import net.spy.memcached.MemcachedNode;
import net.spy.memcached.MemcachedReplicaGroup;
import net.spy.memcached.compat.SpyObject;
import net.spy.memcached.ops.Operation;
import net.spy.memcached.ops.OperationState;

/**
 * Represents a node with the memcached cluster, along with buffering and
 * operation queues.
 */
public abstract class TCPMemcachedNodeImpl extends SpyObject
        implements MemcachedNode {

  private final String name;
  private SocketAddress socketAddress;
  private final ByteBuffer rbuf;
  private final ByteBuffer wbuf;
  protected final BlockingQueue<Operation> writeQ;
  private final BlockingQueue<Operation> readQ;
  private final BlockingQueue<Operation> inputQueue;
  private final long opQueueMaxBlockTime;
  private final AtomicInteger reconnectAttempt = new AtomicInteger(1);
  private boolean isFirstConnecting = true;
  private SocketChannel channel;
  private int toWrite = 0;
  protected Operation optimizedOp = null;
  private volatile SelectionKey sk = null;
  private boolean shouldAuth = false;
  private CountDownLatch authLatch;
  private ArrayList<Operation> reconnectBlocked;
  private String version = null;
  private boolean isAsciiProtocol = true;
  private boolean enabledMGetOp = false;
  private boolean enabledMGetsOp = false;
  private boolean enabledSpaceSeparate = false;

  // operation Future.get timeout counter
  private final AtomicInteger continuousTimeout = new AtomicInteger(0);
  private final AtomicLong timeoutStartNanos = new AtomicLong(0);
  private boolean toRatioEnabled = false;
  private int[] toCountArray;
  private final static int MAX_TOCOUNT = 100;   /* to count array size */
  private int toCountIdx;         /* to count array index */
  private int toRatioMax;         /* maximum timeout ratio */
  private int toRatioNow;         /* current timeout ratio */
  private final Lock toRatioLock = new ReentrantLock();

  // # of operations added into inputQueue as a hint.
  private final AtomicLong addOpCount;

  /* ENABLE_REPLICATION if */
  private MemcachedReplicaGroup replicaGroup;
  /* ENABLE_REPLICATION end */

  private void resetTimeoutRatioCount() {
    if (toRatioEnabled) {
      toRatioLock.lock();
      for (int i = 0; i < MAX_TOCOUNT; i++) {
        toCountArray[i] = 0;
      }
      toCountIdx = -1;
      toRatioMax = 0;
      toRatioNow = 0;
      toRatioLock.unlock();
    }
  }

  private void addTimeoutRatioCount(boolean timedOut) {
    if (toRatioEnabled) {
      toRatioLock.lock();
      if ((++toCountIdx) >= MAX_TOCOUNT) {
        toCountIdx = 0;
      }
      if (toCountArray[toCountIdx] > 0) {
        toRatioNow -= toCountArray[toCountIdx];
        toCountArray[toCountIdx] = 0;
      }
      if (timedOut) {
        toCountArray[toCountIdx] = 1;
        toRatioNow += 1;
        if (toRatioNow > toRatioMax) {
          toRatioMax = toRatioNow;
        }
      }
      toRatioLock.unlock();
    }
  }

  public TCPMemcachedNodeImpl(String name,
                              SocketAddress sa,
                              int bufSize, BlockingQueue<Operation> rq,
                              BlockingQueue<Operation> wq, BlockingQueue<Operation> iq,
                              long opQueueMaxBlockTime, boolean waitForAuth,
                              boolean asciiProtocol) {
    super();
    assert sa != null : "No SocketAddress";
    assert bufSize > 0 : "Invalid buffer size: " + bufSize;
    assert rq != null : "No operation read queue";
    assert wq != null : "No operation write queue";
    assert iq != null : "No input queue";

    this.name = name;
    setSocketAddress(sa);
    rbuf = ByteBuffer.allocate(bufSize);
    wbuf = ByteBuffer.allocate(bufSize);
    ((Buffer) getWbuf()).clear();
    readQ = rq;
    writeQ = wq;
    inputQueue = iq;
    addOpCount = new AtomicLong(0);
    this.opQueueMaxBlockTime = opQueueMaxBlockTime;
    shouldAuth = waitForAuth;
    isAsciiProtocol = asciiProtocol;
    authLatch = new CountDownLatch(shouldAuth ? 1 : 0);
  }

  public final void copyInputQueue() {
    Collection<Operation> tmp = new ArrayList<>();

    // don't drain more than we have space to place
    inputQueue.drainTo(tmp, writeQ.remainingCapacity());

    writeQ.addAll(tmp);
  }

  public Collection<Operation> destroyInputQueue() {
    Collection<Operation> rv = new ArrayList<>();
    inputQueue.drainTo(rv);
    return rv;
  }

  private Collection<Operation> destroyQueue(BlockingQueue<Operation> queue, boolean resend) {
    Collection<Operation> rv = new ArrayList<>();
    queue.drainTo(rv);
    if (resend) {
      for (Operation o : rv) {
        o.reset();
      }
    }

    return rv;
  }

  public Collection<Operation> destroyWriteQueue(boolean resend) {
    return destroyQueue(writeQ, resend);
  }

  public Collection<Operation> destroyReadQueue(boolean resend) {
    return destroyQueue(readQ, resend);
  }

  public final void setupResend(String cause) {
    // First, reset the current write op, or cancel it if we should
    // be authenticating
    Operation op = getCurrentWriteOp();
    if (shouldAuth && op != null) {
      /*
       * Do not cancel the operation.
       * There is no reason to cancel it first
       * and it will be cancelled in the code below.
       */
    } else if (op != null) {
      if (op.getBuffer() != null) {
        op.reset();
      } else {
        /* This case cannot happen. */
        getLogger().warn("No buffer for current write op, removing");
        removeCurrentWriteOp();
      }
    }

    // Now cancel all the pending read operations.  Might be better to
    // to requeue them.
    while (hasReadOp()) {
      op = removeCurrentReadOp();
      if (op != getCurrentWriteOp()) {
        getLogger().warn("Discarding partially completed op: %s", op);
        op.cancel(cause);
      }
    }

    while (shouldAuth && hasWriteOp()) {
      op = removeCurrentWriteOp();
      getLogger().warn("Discarding partially completed op: %s", op);
      op.cancel(cause);
    }

    ((Buffer) getWbuf()).clear();
    ((Buffer) getRbuf()).clear();
    toWrite = 0;
  }

  // Prepare the pending operations.  Return true if there are any pending
  // ops
  private void preparePending() {
    // Copy the input queue into the write queue.
    copyInputQueue();

    // Now check the ops
    Operation nextOp = getCurrentWriteOp();
    while (nextOp != null && nextOp.isCancelled()) {
      getLogger().info("Removing cancelled operation: %s", nextOp);
      removeCurrentWriteOp();
      nextOp = getCurrentWriteOp();
    }
  }

  public final void fillWriteBuffer(boolean shouldOptimize) {
    if (toWrite == 0 && readQ.remainingCapacity() > 0) {
      ((Buffer) getWbuf()).clear();
      Operation o = getNextWritableOp();
      while (o != null && toWrite < getWbuf().capacity()) {
        assert o.getState() == OperationState.WRITING;

        ByteBuffer obuf = o.getBuffer();
        assert obuf != null : "Didn't get a write buffer from " + o;
        int bytesToCopy = Math.min(getWbuf().remaining(),
                obuf.remaining());
        byte[] b = new byte[bytesToCopy];
        obuf.get(b);
        getWbuf().put(b);
        getLogger().debug("After copying stuff from %s: %s",
                o, getWbuf());
        if (!o.getBuffer().hasRemaining()) {
          o.writeComplete();
          transitionWriteItem();

          preparePending();
          if (shouldOptimize) {
            optimize();
          }

          if (readQ.remainingCapacity() > 0) {
            o = getNextWritableOp();
          } else {
            o = null;
          }
        }
        toWrite += bytesToCopy;
      }
      ((Buffer) getWbuf()).flip();
      assert toWrite <= getWbuf().capacity()
              : "toWrite exceeded capacity: " + this;
      assert toWrite == getWbuf().remaining()
              : "Expected " + toWrite + " remaining, got "
              + getWbuf().remaining();
    } else {
      getLogger().debug("Buffer is full, skipping");
    }
  }

  public final void transitionWriteItem() {
    Operation op = removeCurrentWriteOp();
    assert op != null : "There is no write item to transition";
    getLogger().debug("Finished writing %s", op);
  }

  protected abstract void optimize();

  public final Operation getCurrentReadOp() {
    return readQ.peek();
  }

  public final Operation removeCurrentReadOp() {
    return readQ.remove();
  }

  public final Operation getCurrentWriteOp() {
    return optimizedOp == null ? writeQ.peek() : optimizedOp;
  }

  private Operation getNextWritableOp() {
    Operation o = getCurrentWriteOp();
    while (o != null && o.getState() == OperationState.WRITE_QUEUED) {
      if (o.isCancelled()) {
        getLogger().debug("Not writing cancelled op.");
        Operation cancelledOp = removeCurrentWriteOp();
        assert o == cancelledOp;
      } else {
        o.setStartTime(System.nanoTime());
        o.writing();
        readQ.add(o);
        return o;
      }
      o = getCurrentWriteOp();
    }
    return o;
  }

  public final Operation removeCurrentWriteOp() {
    Operation rv = optimizedOp;
    if (rv == null) {
      rv = writeQ.remove();
    } else {
      optimizedOp = null;
    }
    return rv;
  }

  public final boolean hasReadOp() {
    return !readQ.isEmpty();
  }

  public final boolean hasWriteOp() {
    return !(optimizedOp == null && writeQ.isEmpty());
  }

  public final void addOpToInputQ(Operation op) {
    op.setHandlingNode(this);
    op.initialize();
    try {
      if (!authLatch.await(1, TimeUnit.SECONDS)) {
        op.cancel("authentication timeout");
        getLogger().warn(
                "Operation canceled because authentication " +
                        "or reconnection and authentication has " +
                        "taken more than one second to complete.");
        getLogger().debug("Canceled operation %s", op.toString());
        return;
      }
      if (!inputQueue.offer(op, opQueueMaxBlockTime,
              TimeUnit.MILLISECONDS)) {
        throw new IllegalStateException("Timed out waiting to add "
                + op + "(max wait=" + opQueueMaxBlockTime + "ms)");
      }
      addOpCount.incrementAndGet();
    } catch (InterruptedException e) {
      // Restore the interrupted status
      Thread.currentThread().interrupt();
      throw new IllegalStateException("Interrupted while waiting to add "
              + op);
    }
  }

  public final boolean addOpToWriteQ(Operation op) {
    op.setHandlingNode(this);
    op.reset();
    if (!writeQ.offer(op)) {
      op.cancel("write queue overflow");
      return false;
    }
    addOpCount.incrementAndGet();
    return true;
  }

  public final void insertOp(Operation op) {
    op.setHandlingNode(this);
    op.initialize();
    ArrayList<Operation> tmp = new ArrayList<>(
            inputQueue.size() + 1);
    tmp.add(op);
    inputQueue.drainTo(tmp);
    inputQueue.addAll(tmp);
    addOpCount.incrementAndGet();
  }

  public final int getSelectionOps() {
    int rv = 0;
    if (getChannel().isConnected()) {
      if (hasReadOp()) {
        rv |= SelectionKey.OP_READ;
      }
      if (toWrite > 0 || hasWriteOp()) {
        rv |= SelectionKey.OP_WRITE;
      }
    } else {
      rv = SelectionKey.OP_CONNECT;
    }
    return rv;
  }

  public final String getNodeName() {
    return name + " " + AddrUtil.getSocketAddressString(socketAddress);
  }

  public final ByteBuffer getRbuf() {
    return rbuf;
  }

  public final ByteBuffer getWbuf() {
    return wbuf;
  }

  private final void setSocketAddress(SocketAddress sa) {
    /* ENABLE_REPLICATION if */
    if (sa instanceof ArcusReplNodeAddress) {
      socketAddress = new ArcusReplNodeAddress((ArcusReplNodeAddress) sa);
      return;
    }
    /* ENABLE_REPLICATION end */
    socketAddress = sa;
  }

  public final SocketAddress getSocketAddress() {
    return socketAddress;
  }

  public final boolean isActive() {
    return reconnectAttempt.get() == 0 && getChannel() != null && getChannel().isConnected();
  }

  public final boolean isFirstConnecting() {
    return isFirstConnecting;
  }

  public final void reconnecting() {
    reconnectAttempt.incrementAndGet();
    isFirstConnecting = false;
    continuousTimeout.set(0);
    timeoutStartNanos.set(0);
    resetTimeoutRatioCount();
  }

  public final void connected() {
    reconnectAttempt.set(0);
    isFirstConnecting = false;
    continuousTimeout.set(0);
    timeoutStartNanos.set(0);
    resetTimeoutRatioCount();
  }

  public final int getReconnectCount() {
    return reconnectAttempt.get();
  }

  @Override
  public final String toString() {
    int sops = 0;
    if (getSk() != null && getSk().isValid()) {
      sops = getSk().interestOps();
    }
    return "{QA name=" + getNodeName()
            + ", #Tops=" + addOpCount
            + ", #iq=" + getInputQueueSize()
            + ", #Wops=" + getWriteQueueSize()
            + ", #Rops=" + getReadQueueSize()
            + ", #CT=" + getContinuousTimeout()
            + ", #TD=" + getTimeoutDuration()
            + ", #TR=" + getTimeoutRatioNow()
            + ", topRop=" + getCurrentReadOp()
            + ", topWop=" + getCurrentWriteOp()
            + ", toWrite=" + toWrite
            + ", interested=" + sops + "}";
  }

  public final void setChannel(SocketChannel to) {
    assert channel == null || !channel.isOpen()
            : "Attempting to overwrite channel";
    channel = to;
  }

  public final SocketChannel getChannel() {
    return channel;
  }

  public final void setSk(SelectionKey to) {
    sk = to;
  }

  public final SelectionKey getSk() {
    return sk;
  }

  public final void setVersion(String vr) {
    version = vr;
    StringTokenizer tokens = new StringTokenizer(version, ".");
    int majorVersion = Integer.parseInt(tokens.nextToken());
    int minorVersion = Integer.parseInt(tokens.nextToken());
    boolean isEnterprise = version.contains("E");
    if (isAsciiProtocol) {
      setEnableMGetOp(majorVersion, minorVersion, isEnterprise);
      setEnableMGetsOp(majorVersion, minorVersion, isEnterprise);
      setEnableSpaceSeparate(majorVersion, minorVersion, isEnterprise);
    }
  }

  public final String getVersion() {
    return version;
  }

  private void setEnableMGetOp(int majorVersion, int minorVersion, boolean isEnterprise) {
    if (isEnterprise) {
      enabledMGetOp = (majorVersion > 0 || (majorVersion == 0 && minorVersion > 6));
    } else {
      enabledMGetOp = (majorVersion > 1 || (majorVersion == 1 && minorVersion > 10));
    }
  }

  private void setEnableMGetsOp(int majorVersion, int minorVersion, boolean isEnterprise) {
    if (isEnterprise) {
      enabledMGetsOp = (majorVersion > 0 || (majorVersion == 0 && minorVersion > 8));
    } else {
      enabledMGetsOp = (majorVersion > 1 || (majorVersion == 1 && minorVersion > 12));
    }
  }

  private void setEnableSpaceSeparate(int majorVersion, int minorVersion,
                                            boolean isEnterprise) {
    if (isEnterprise) {
      enabledSpaceSeparate = (majorVersion > 0 || (majorVersion == 0 && minorVersion > 6));
    } else {
      enabledSpaceSeparate = (majorVersion > 1 || (majorVersion == 1 && minorVersion > 10));
    }
  }

  public final boolean enabledMGetOp() {
    return enabledMGetOp;
  }

  public final boolean enabledMGetsOp() {
    return enabledMGetsOp;
  }

  public final boolean enabledSpaceSeparate() {
    return enabledSpaceSeparate;
  }

  public final int getBytesRemainingToWrite() {
    return toWrite;
  }

  public final int writeSome() throws IOException {
    int wrote = channel.write(wbuf);
    assert wrote >= 0 : "Wrote negative bytes?";
    toWrite -= wrote;
    assert toWrite >= 0
            : "toWrite went negative after writing " + wrote
            + " bytes for " + this;
    getLogger().debug("Wrote %d bytes", wrote);
    return wrote;
  }

  public void setContinuousTimeout(boolean timedOut) {
    if (isActive()) {
      addTimeoutRatioCount(timedOut);
    }
    if (timedOut && isActive()) {
      if (timeoutStartNanos.get() == 0) {
        timeoutStartNanos.set(System.nanoTime());
      }
      continuousTimeout.incrementAndGet();
    } else {
      timeoutStartNanos.set(0);
      continuousTimeout.set(0);
    }
  }

  public int getContinuousTimeout() {
    return continuousTimeout.get();
  }

  public void enableTimeoutRatio() {
    toRatioEnabled = true;
    toCountArray = new int[MAX_TOCOUNT];
    resetTimeoutRatioCount();
  }

  public int getTimeoutRatioNow() {
    int ratio = -1; // invalid
    if (toRatioEnabled) {
      toRatioLock.lock();
      ratio = toRatioNow;
      toRatioLock.unlock();
    }
    return ratio;
  }

  public long getTimeoutDuration() {
    long tn = timeoutStartNanos.get();
    if (tn == 0) {
      return 0;
    }
    return TimeUnit.MILLISECONDS.convert(
        System.nanoTime() - tn,
        TimeUnit.NANOSECONDS);
  }

  public final void fixupOps() {
    // As the selection key can be changed at any point due to node
    // failure, we'll grab the current volatile value and configure it.
    SelectionKey s = sk;
    if (s != null && s.isValid()) {
      int iops = getSelectionOps();
      getLogger().debug("Setting interested opts to %d", iops);
      s.interestOps(iops);
    } else {
      getLogger().debug("Selection key is not valid.");
    }
  }

  public final void authComplete() {
    if (reconnectBlocked != null && !reconnectBlocked.isEmpty()) {
      inputQueue.addAll(reconnectBlocked);
    }
    authLatch.countDown();
  }

  public final void setupForAuth(String cause) {
    if (shouldAuth) {
      authLatch = new CountDownLatch(1);
      if (!inputQueue.isEmpty()) {
        reconnectBlocked = new ArrayList<>(
                inputQueue.size() + 1);
        inputQueue.drainTo(reconnectBlocked);
      }
      assert (inputQueue.isEmpty());
      setupResend(cause);
    } else {
      authLatch = new CountDownLatch(0);
    }
  }

  public void closeChannel() throws IOException {
    if (sk != null) {
      sk.cancel();
      sk.attach(null);
      sk = null;
    }
    try {
      if (channel != null) {
        channel.close();
        if (toWrite > 0) {
          getLogger().warn(
              "Shut down with %d bytes remaining to write",
              toWrite);
        }
        getLogger().debug("Shut down channel %s", channel);
      }
    } finally {
      channel = null;
    }
  }

  public final void shutdown() throws IOException {
    closeChannel();
  }

  public int getInputQueueSize() {
    return inputQueue.size();
  }

  public int getWriteQueueSize() {
    return writeQ.size() + (optimizedOp == null ? 0 : 1);
  }

  public int getReadQueueSize() {
    return readQ.size();
  }

  @Override
  public String getOpQueueStatus() {
    return "#Tops=" + addOpCount +
        " #iq=" + getInputQueueSize() +
        " #Wops=" + getWriteQueueSize() +
        " #Rops=" + getReadQueueSize() +
        " #CT=" + getContinuousTimeout() +
        " #TD=" + getTimeoutDuration() +
        " #TR=" + getTimeoutRatioNow();
  }

  /* ENABLE_REPLICATION if */
  public void setReplicaGroup(MemcachedReplicaGroup g) {
    replicaGroup = g;
  }

  public MemcachedReplicaGroup getReplicaGroup() {
    return replicaGroup;
  }

  private BlockingQueue<Operation> getAllOperations(boolean cancelNonIdempotent) {
    BlockingQueue<Operation> allOp = new LinkedBlockingQueue<>();

    while (hasReadOp()) {
      Operation op = removeCurrentReadOp();
      if (op == getCurrentWriteOp()) {
        /* Operation could exist both writeQ and readQ,
         * if all bytes of the operation have not been written yet.
         */
      } else if (cancelNonIdempotent && !op.isIdempotentOperation()) {
        op.cancel("by moving idempotent operations only");
      } else {
        allOp.add(op);
      }
    }

    if (optimizedOp != null) {
      allOp.add(optimizedOp);
      optimizedOp = null;
    }

    if (!writeQ.isEmpty()) {
      writeQ.drainTo(allOp);
    }

    if (!inputQueue.isEmpty()) {
      inputQueue.drainTo(allOp);
    }

    return allOp;
  }

  public int moveOperations(final MemcachedNode toNode, boolean cancelNonIdempotent) {
    BlockingQueue<Operation> allOp = getAllOperations(cancelNonIdempotent);
    int opCount = allOp.size();
    int movedOpCount = 0;

    if (opCount > 0) {
      for (Operation op : allOp) {
        movedOpCount += toNode.addOpToWriteQ(op) ? 1 : 0;
      }
      getLogger().info("Total %d operations have been moved to %s "
              + "and %d operations have been canceled.",
          movedOpCount, toNode, opCount - movedOpCount);
    }

    return movedOpCount;
  }

  public boolean hasNonIdempotentOperationInReadQ() {
    for (Operation op : readQ) {
      if (op == getCurrentWriteOp()) {
        continue;
      }
      if (!op.isIdempotentOperation()) {
        return true;
      }
    }
    return false;
  }
  /* ENABLE_REPLICATION end */
}
