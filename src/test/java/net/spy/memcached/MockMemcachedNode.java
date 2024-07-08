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
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.util.Collection;

import net.spy.memcached.ops.Operation;

public class MockMemcachedNode implements MemcachedNode {
  private final InetSocketAddress socketAddress;

  public SocketAddress getSocketAddress() {
    return socketAddress;
  }

  public MockMemcachedNode(InetSocketAddress socketAddress) {
    this.socketAddress = socketAddress;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    MockMemcachedNode that = (MockMemcachedNode) o;

    if (socketAddress != null
            ? !socketAddress.equals(that.socketAddress)
            : that.socketAddress != null) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    return (socketAddress != null ? socketAddress.hashCode() : 0);
  }

  public void copyInputQueue() {
    // noop
  }

  public void setupResend(String cause) {
    // noop
  }

  public void fillWriteBuffer(boolean optimizeGets) {
    // noop
  }

  public void transitionWriteItem() {
    // noop
  }

  public Operation getCurrentReadOp() {
    return null;
  }

  public Operation removeCurrentReadOp() {
    return null;
  }

  public Operation getCurrentWriteOp() {
    return null;
  }

  public Operation removeCurrentWriteOp() {
    return null;
  }

  public boolean hasReadOp() {
    return false;
  }

  public boolean hasWriteOp() {
    return false;
  }

  public boolean hasOp() {
    return false;
  }

  public void addOpToInputQ(Operation op) {
    // noop
  }

  public boolean addOpToWriteQ(Operation op) {
    // noop
    return false;
  }

  public void insertOp(Operation op) {
    // noop
  }

  public int getSelectionOps() {
    return 0;
  }

  @Override
  public String getNodeName() {
    return AddrUtil.getSocketAddressString(socketAddress);
  }

  public ByteBuffer getRbuf() {
    return null;
  }

  public ByteBuffer getWbuf() {
    return null;
  }

  public boolean isActive() {
    return false;
  }

  public boolean isFirstConnecting() {
    return false;
  }

  public void reconnecting() {
    // noop
  }

  public void connected() {
    // noop
  }

  public int getReconnectCount() {
    return 0;
  }

  public void setChannel(SocketChannel to) {
    // noop
  }

  public SocketChannel getChannel() {
    return null;
  }

  public void setSk(SelectionKey to) {
    // noop
  }

  public SelectionKey getSk() {
    return null;
  }

  public void setVersion(String vr) {
    // noop
  }

  public String getVersion() {
    return null;
  }

  public boolean enabledMGetOp() {
    return false;
  }

  @Override
  public boolean enabledMGetsOp() {
    return false;
  }

  public boolean enabledSpaceSeparate() {
    return false;
  }

  public int getBytesRemainingToWrite() {
    return 0;
  }

  public int writeSome() throws IOException {
    return 0;
  }

  public void fixupOps() {
    // noop
  }

  public Collection<Operation> destroyInputQueue() {
    return null;
  }

  public Collection<Operation> destroyWriteQueue(boolean resend) {
    return null;
  }

  public Collection<Operation> destroyReadQueue(boolean resend) {
    return null;
  }

  public void authComplete() {
    // noop
  }

  public void setupForAuth(String cause) {
    // noop
  }

  public int getContinuousTimeout() {
    return 0;
  }

  public void setContinuousTimeout(boolean timedOut) {
    // noop
  }

  public void enableTimeoutRatio() {
    // noop
  }

  public int getTimeoutRatioNow() {
    return -1; // disabled
  }

  @Override
  public long getTimeoutDuration() {
    return 0;
  }

  @Override
  public void closeChannel() throws IOException {
    // noop
  }

  public void shutdown() throws IOException {
    // noop
  }

  @Override
  public String getOpQueueStatus() {
    return "MOCK_STATE";
  }
  /* ENABLE_REPLICATION if */

  @Override
  public void setReplicaGroup(MemcachedReplicaGroup g) {
    // noop
  }

  @Override
  public MemcachedReplicaGroup getReplicaGroup() {
    // noop
    return null;
  }

  @Override
  public int moveOperations(final MemcachedNode toNode, boolean cancelNonIdempotent) {
    // noop
    return 0;
  }

  @Override
  public boolean hasNonIdempotentOperationInReadQ() {
    // noop
    return false;
  }
  /* ENABLE_REPLICATION end */
}
