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
/**
 *
 */
package net.spy.memcached;

import java.io.IOException;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.util.Collection;
import java.util.concurrent.BlockingQueue;

import net.spy.memcached.ops.Operation;

public class MemcachedNodeROImpl implements MemcachedNode {

  private final MemcachedNode root;

  public MemcachedNodeROImpl(MemcachedNode n) {
    super();
    root = n;
  }

  @Override
  public String toString() {
    return root.toString();
  }

  public MemcachedNode getMemcachedNode() {
    return root;
  }

  public void addOp(Operation op) {
    throw new UnsupportedOperationException();
  }

  public void insertOp(Operation op) {
    throw new UnsupportedOperationException();
  }

  public void connected() {
    throw new UnsupportedOperationException();
  }

  public void copyInputQueue() {
    throw new UnsupportedOperationException();
  }

  public void fillWriteBuffer(boolean optimizeGets) {
    throw new UnsupportedOperationException();
  }

  public void fixupOps() {
    throw new UnsupportedOperationException();
  }

  public int getBytesRemainingToWrite() {
    return root.getBytesRemainingToWrite();
  }

  public SocketChannel getChannel() {
    throw new UnsupportedOperationException();
  }

  public Operation getCurrentReadOp() {
    throw new UnsupportedOperationException();
  }

  public Operation getCurrentWriteOp() {
    throw new UnsupportedOperationException();
  }

  public ByteBuffer getRbuf() {
    throw new UnsupportedOperationException();
  }

  public int getReconnectCount() {
    return root.getReconnectCount();
  }

  public int getSelectionOps() {
    return root.getSelectionOps();
  }

  public SelectionKey getSk() {
    throw new UnsupportedOperationException();
  }

  public SocketAddress getSocketAddress() {
    return root.getSocketAddress();
  }

  public ByteBuffer getWbuf() {
    throw new UnsupportedOperationException();
  }

  public boolean hasReadOp() {
    return root.hasReadOp();
  }

  public boolean hasWriteOp() {
    return root.hasReadOp();
  }

  public boolean isActive() {
    return root.isActive();
  }

  public void reconnecting() {
    throw new UnsupportedOperationException();
  }

  public void registerChannel(SocketChannel ch, SelectionKey selectionKey) {
    throw new UnsupportedOperationException();
  }

  public Operation removeCurrentReadOp() {
    throw new UnsupportedOperationException();
  }

  public Operation removeCurrentWriteOp() {
    throw new UnsupportedOperationException();
  }

  public void setChannel(SocketChannel to) {
    throw new UnsupportedOperationException();
  }

  public void setSk(SelectionKey to) {
    throw new UnsupportedOperationException();
  }

  public void setVersion(String vr) {
    throw new UnsupportedOperationException();
  }

  public String getVersion() {
    throw new UnsupportedOperationException();
  }

  public boolean enabledMGetOp() {
    throw new UnsupportedOperationException();
  }

  public boolean enabledSpaceSeparate() {
    throw new UnsupportedOperationException();
  }

  public void setupResend(boolean cancelWrite, String cause) {
    throw new UnsupportedOperationException();
  }

  public void transitionWriteItem() {
    throw new UnsupportedOperationException();
  }

  public int writeSome() throws IOException {
    throw new UnsupportedOperationException();
  }

  public Collection<Operation> destroyInputQueue() {
    throw new UnsupportedOperationException();
  }

  public Collection<Operation> destroyWriteQueue(boolean resend) {
    throw new UnsupportedOperationException();
  }

  public Collection<Operation> destroyReadQueue(boolean resend) {
    throw new UnsupportedOperationException();
  }

  public void authComplete() {
    throw new UnsupportedOperationException();
  }

  public void setupForAuth(String cause) {
    throw new UnsupportedOperationException();
  }

  public int getContinuousTimeout() {
    throw new UnsupportedOperationException();
  }

  public void setContinuousTimeout(boolean isIncrease) {
    throw new UnsupportedOperationException();
  }

  public void enableTimeoutRatio() {
    throw new UnsupportedOperationException();
  }

  public int getTimeoutRatioNow() {
    throw new UnsupportedOperationException();
  }

  public boolean isFake() {
    throw new UnsupportedOperationException();
  }

  public void shutdown() throws IOException {
    throw new UnsupportedOperationException();
  }

  public String getStatus() {
    throw new UnsupportedOperationException();
  }

  /* ENABLE_REPLICATION if */
  public void setReplicaGroup(MemcachedReplicaGroup g) {
    throw new UnsupportedOperationException();
  }

  public MemcachedReplicaGroup getReplicaGroup() {
    throw new UnsupportedOperationException();
  }

  public void addAllOpToInputQ(BlockingQueue<Operation> allOp) {
    throw new UnsupportedOperationException();
  }

  public int moveOperations(final MemcachedNode toNode) {
    throw new UnsupportedOperationException();
  }
  /* ENABLE_REPLICATION end */
}
