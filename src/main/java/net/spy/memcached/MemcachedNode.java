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
package net.spy.memcached;

import java.io.IOException;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.util.Collection;

import net.spy.memcached.ops.Operation;

/**
 * Interface defining a connection to a memcached server.
 */
public interface MemcachedNode {

  /**
   * Move all of the operations delivered via addOperation into the internal
   * write queue.
   */
  void copyInputQueue();

  /**
   * Extract all queued items for this node destructively.
   *
   * This is useful for redistributing items.
   */
  Collection<Operation> destroyInputQueue();

  /**
   * Extract all queued items for this node destructively.
   *
   * This is useful for redistributing items.
   */
  Collection<Operation> destroyWriteQueue(boolean resend);

  /**
   * Extract all queued items for this node destructively.
   *
   * This is useful for redistributing items.
   */
  Collection<Operation> destroyReadQueue(boolean resend);

  /**
   * Clear the queue of currently processing operations by either cancelling
   * them or setting them up to be reapplied after a reconnect.
   */
  void setupResend(String cause);

  /**
   * Fill the write buffer with data from the next operations in the queue.
   *
   * @param optimizeGets if true, combine sequential gets into a single
   *                     multi-key get
   */
  void fillWriteBuffer(boolean optimizeGets);

  /**
   * Transition the current write item into a read state.
   */
  void transitionWriteItem();

  /**
   * Get the operation at the top of the queue that is requiring input.
   */
  Operation getCurrentReadOp();

  /**
   * Remove the operation at the top of the queue that is requiring input.
   */
  Operation removeCurrentReadOp();

  /**
   * Get the operation at the top of the queue that has information available
   * to write.
   */
  Operation getCurrentWriteOp();

  /**
   * Remove the operation at the top of the queue that has information
   * available to write.
   */
  Operation removeCurrentWriteOp();

  /**
   * True if an operation is available to read.
   */
  boolean hasReadOp();

  /**
   * True if an operation is available to write.
   */
  boolean hasWriteOp();

  /**
   * True if any operation is in operation queue.
   */
  boolean hasOp();

  /**
   * Add an operation to the queue.  Authentication operations should
   * never be added to the queue, but this is not checked.
   */
  void addOpToInputQ(Operation op);

  /**
   * Add an operation to the write queue. It is used to process operation in prior to
   * operations in input queue. For example, when the switchover occurs,
   * the operation moved to the new master must be prioritized.
   */
  boolean addOpToWriteQ(Operation op);

  /**
   * Insert an operation to the beginning of the queue.
   *
   * This method is meant to be invoked rarely.
   */
  void insertOp(Operation o);

  /**
   * Compute the appropriate selection operations for the channel this
   * MemcachedNode holds to the server.
   */
  int getSelectionOps();

  /**
   * Get the node name used for reading ip, port and hostname from this node
   */
  String getNodeName();

  /**
   * Get the buffer used for reading data from this node.
   */
  ByteBuffer getRbuf();

  /**
   * Get the buffer used for writing data to this node.
   */
  ByteBuffer getWbuf();

  /**
   * Get the SocketAddress of the server to which this node is connected.
   */
  SocketAddress getSocketAddress();

  /**
   * True if this node is <q>active.</q>  i.e. is is currently connected
   * and expected to be able to process requests
   */
  boolean isActive();

  /**
   * True if it is the first connecting attempt.
   */
  boolean isFirstConnecting();

  /**
   * Notify this node that it will be reconnecting.
   */
  void reconnecting();

  /**
   * Notify this node that it has reconnected.
   */
  void connected();

  /**
   * Get the current reconnect count.
   */
  int getReconnectCount();

  /**
   * Set the SocketChannel this node uses.
   */
  void setChannel(SocketChannel to);

  /**
   * Get the SocketChannel for this connection.
   */
  SocketChannel getChannel();

  /**
   * Set the selection key for this node.
   */
  void setSk(SelectionKey to);

  /**
   * Get the selection key from this node.
   */
  SelectionKey getSk();

  /**
   * Set the version information for this node.
   */
  void setVersion(String vr);

  /**
   * Get the version information from this node.
   */
  String getVersion();

  /**
   * Check the enable MGet operation.
   */
  boolean enabledMGetOp();

  /**
   * Check the enable MGets operation.
   */
  boolean enabledMGetsOp();

  /**
   * Check the enable SpaceSeparate operation.
   */
  boolean enabledSpaceSeparate();

  /**
   * Get the number of bytes remaining to write.
   */
  int getBytesRemainingToWrite();

  /**
   * Write some bytes and return the number of bytes written.
   *
   * @return the number of bytes written
   * @throws IOException if there's a problem writing
   */
  int writeSome() throws IOException;

  /**
   * Fix up the selection ops on the selection key.
   */
  void fixupOps();

  /**
   * Let the node know that auth is complete.  Typically this would
   * mean the node can start processing and accept new operations to
   * its input queue.
   */
  void authComplete();

  /**
   * Tell a node to set up for authentication.  Typically this would
   * mean blocking additions to the queue.  In a reconnect situation
   * this may mean putting any queued operations on hold to get to
   * an auth complete state.
   */
  void setupForAuth(String cause);

  /**
   * Count 'time out' exceptions to drop connections that fail perpetually
   *
   * @param timedOut
   */
  void setContinuousTimeout(boolean timedOut);

  int getContinuousTimeout();

  void enableTimeoutRatio();

  int getTimeoutRatioNow();

  long getTimeoutDuration();

  /**
   * Close the socket channel of the node.
   * This method is only used for I/O thread to close channel.
   */
  void closeChannel() throws IOException;

  /**
   * Shutdown the node
   */
  void shutdown() throws IOException;

  /**
   * get operation queue status
   *
   * @return status string
   */
  String getOpQueueStatus();

  /* ENABLE_REPLICATION if */
  void setReplicaGroup(MemcachedReplicaGroup g);

  MemcachedReplicaGroup getReplicaGroup();

  int moveOperations(final MemcachedNode toNode, boolean cancelNonIdempotent);

  boolean hasNonIdempotentOperationInReadQ();
  /* ENABLE_REPLICATION end */
}
