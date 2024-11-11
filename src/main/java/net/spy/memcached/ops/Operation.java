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
package net.spy.memcached.ops;

import java.io.IOException;
import java.nio.ByteBuffer;

import net.spy.memcached.MemcachedNode;
import net.spy.memcached.RedirectHandler;


/**
 * Base interface for all operations.
 */
public interface Operation {

  /**
   * Has this operation been cancelled?
   */
  boolean isCancelled();

  /**
   * True if an error occurred while processing this operation.
   */
  boolean hasErrored();

  /**
   * Get the exception that occurred (or null if no exception occurred).
   */
  OperationException getException();

  /**
   * Get the callback for this get operation.
   */
  OperationCallback getCallback();

  /**
   * Cancel this operation.
   */
  boolean cancel(String cause);

  /**
   * Get the cause of cancel.
   */
  String getCancelCause();

  /**
   * Get the current state of this operation.
   */
  OperationState getState();

  /* ENABLE_REPLICATION if */
  /**
   * reset operation state to WRITE_QUEUED
   */
  void reset();
  /* ENABLE_REPLICATION end */

  /**
   * Get the write buffer for this operation.
   */
  ByteBuffer getBuffer();

  /**
   * Invoked when we start writing all of the bytes from this operation to
   * the sockets write buffer.
   */
  void writing();

  /**
   * Invoked after having written all of the bytes from the supplied output
   * buffer.
   */
  void writeComplete();

  /**
   * Initialize this operation.  This is used to prepare output byte buffers
   * and stuff.
   */
  void initialize();

  /**
   * Read data from the given byte buffer and dispatch to the appropriate
   * read mechanism.
   */
  void readFromBuffer(ByteBuffer data) throws IOException;

  /**
   * Handle a raw data read.
   */
  void handleRead(ByteBuffer data);

  /**
   * Get the node that should've been handling this operation.
   */
  MemcachedNode getHandlingNode();

  /**
   * Set a reference to the node that will be/is handling this operation.
   *
   * @param to a memcached node
   */
  void setHandlingNode(MemcachedNode to);

  OperationType getOperationType();

  boolean isWriteOperation();

  boolean isReadOperation();

  boolean isBulkOperation();

  boolean isPipeOperation();

  boolean isIdempotentOperation();

  /* ENABLE_MIGRATION if */
  RedirectHandler getAndClearRedirectHandler();
  /* ENABLE_MIGRATION end */

  APIType getAPIType();

  void setStartTime(long startTime);

  long getStartTime();
}
