package net.spy.memcached.ops;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;

import net.spy.memcached.MemcachedNode;


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
  void cancel(String cause);

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
   * reset operation state to WRITING
   */
  void resetState();

  void setMoved(boolean s);

  /* ENABLE_REPLICATION end */
  /* ENABLE_MIGRATION if */

  /**
   * Get the response string of Migration from server(memcached node).
   */
  String getMgResponse(int index);

  /**
   * Get the response size of Migration from server(memcached node).
   */
  int getMgResponseSize();
  /* ENABLE_MIGRATION end */

  /**
   * Get the write buffer for this operation.
   */
  ByteBuffer getBuffer();

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

  APIType getAPIType();

  /* ENABLE_MIGRATION if */
  void setMigratingCount(int count);

  void decrMigratingCount(String line);

  void decrMigratingCount(String key, String line);

  Map<String, Object> getArguments();
  /* ENABLE_MIGRATION end */
}
