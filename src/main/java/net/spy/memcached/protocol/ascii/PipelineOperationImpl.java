/*
 * arcus-java-client : Arcus Java client
 * Copyright 2010-2014 NAVER Corp.
 * Copyright 2014-present JaM2in Co., Ltd.
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
package net.spy.memcached.protocol.ascii;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

import net.spy.memcached.ops.APIType;
import net.spy.memcached.ops.KeyedOperation;
import net.spy.memcached.ops.Operation;
import net.spy.memcached.ops.OperationCallback;
import net.spy.memcached.ops.OperationErrorType;
import net.spy.memcached.ops.OperationException;
import net.spy.memcached.ops.OperationState;
import net.spy.memcached.ops.OperationStatus;
import net.spy.memcached.ops.OperationType;
import net.spy.memcached.ops.PipelineOperation;
import net.spy.memcached.ops.StatusCode;

/**
 * Operation for executing multiple commands as pipeline.
 */
public final class PipelineOperationImpl extends OperationImpl implements PipelineOperation {

  // PIPE RESPONSES
  private static final OperationStatus END =
      new OperationStatus(true, "END", StatusCode.SUCCESS);
  private static final OperationStatus FAILED_END =
      new OperationStatus(false, "FAILED_END", StatusCode.ERR_FAILED_END);

  // EACH COMMAND'S SUCCEED RESPONSES
  private static final OperationStatus CREATED_STORED =
      new OperationStatus(true, "CREATED_STORED", StatusCode.SUCCESS);
  private static final OperationStatus STORED =
      new OperationStatus(true, "STORED", StatusCode.SUCCESS);
  private static final OperationStatus REPLACED =
      new OperationStatus(true, "REPLACED", StatusCode.SUCCESS);
  private static final OperationStatus UPDATED =
      new OperationStatus(true, "UPDATED", StatusCode.SUCCESS);
  private static final OperationStatus EXIST =
      new OperationStatus(true, "EXIST", StatusCode.EXIST);
  private static final OperationStatus NOT_EXIST =
      new OperationStatus(true, "NOT_EXIST", StatusCode.NOT_EXIST);
  private static final OperationStatus DELETED =
      new OperationStatus(true, "DELETED", StatusCode.SUCCESS);
  private static final OperationStatus DELETED_DROPPED =
      new OperationStatus(true, "DELETED_DROPPED", StatusCode.SUCCESS);

  // EACH COMMAND'S FAILED RESPONSES
  private static final OperationStatus NOT_FOUND =
      new OperationStatus(false, "NOT_FOUND", StatusCode.ERR_NOT_FOUND);
  private static final OperationStatus NOT_FOUND_ELEMENT =
      new OperationStatus(false, "NOT_FOUND_ELEMENT", StatusCode.ERR_NOT_FOUND_ELEMENT);
  private static final OperationStatus NOTHING_TO_UPDATE =
      new OperationStatus(false, "NOTHING_TO_UPDATE", StatusCode.ERR_NOTHING_TO_UPDATE);
  private static final OperationStatus ELEMENT_EXISTS =
      new OperationStatus(false, "ELEMENT_EXISTS", StatusCode.ERR_ELEMENT_EXISTS);
  private static final OperationStatus OVERFLOWED =
      new OperationStatus(false, "OVERFLOWED", StatusCode.ERR_OVERFLOWED);
  private static final OperationStatus OUT_OF_RANGE =
      new OperationStatus(false, "OUT_OF_RANGE", StatusCode.ERR_OUT_OF_RANGE);
  private static final OperationStatus TYPE_MISMATCH =
      new OperationStatus(false, "TYPE_MISMATCH", StatusCode.ERR_TYPE_MISMATCH);
  private static final OperationStatus BKEY_MISMATCH =
      new OperationStatus(false, "BKEY_MISMATCH", StatusCode.ERR_BKEY_MISMATCH);
  private static final OperationStatus EFLAG_MISMATCH =
      new OperationStatus(false, "EFLAG_MISMATCH", StatusCode.ERR_EFLAG_MISMATCH);
  private static final OperationStatus UNREADABLE =
      new OperationStatus(false, "UNREADABLE", StatusCode.ERR_UNREADABLE);

  private final List<KeyedOperation> ops;
  private final List<String> keys;
  private final PipelineOperation.Callback cb;

  private int responseIndex = 0;
  private boolean expectingResponse = false;
  private boolean successAll = true;

  /**
   * @param ops  each command's operation to be pipelined
   * @param keys keys involved in this pipeline operation without duplicate
   * @param cb   callback for this pipeline operation
   */
  public PipelineOperationImpl(List<KeyedOperation> ops, List<String> keys,
                               OperationCallback cb) {
    super(cb);
    if (ops == null || ops.isEmpty()) {
      throw new IllegalArgumentException("Ops cannot be null or empty");
    }
    this.ops = ops;
    this.keys = keys;
    this.cb = (PipelineOperation.Callback) cb;
    setAPIType(APIType.PIPE);
    setOperationType(OperationType.WRITE);
  }

  /**
   * Make a pipelined command buffer using each command's buffer.
   */
  @Override
  public void initialize() {
    // 1) Initialize operations and collect each buffers
    // to handle switchover/redirect single key situations,
    // make buffer from responseIndex Operation
    int opCount = ops.size() - responseIndex;
    ByteBuffer[] buffers = new ByteBuffer[opCount];
    int bufferCount = 0;
    for (int i = responseIndex; i < ops.size(); i++) {
      Operation op = ops.get(i);
      op.initialize();
      ByteBuffer buffer = op.getBuffer();
      if (buffer != null && buffer.hasRemaining()) {
        buffers[bufferCount++] = buffer;
      }
    }

    // 2) Remove "pipe" from the last command buffer
    if (bufferCount > 0) {
      buffers[bufferCount - 1] = removePipeFromLastBuffer(buffers[bufferCount - 1]);
    }

    // 3) Create a concatenated pipedBuffer
    int totalSize = 0;
    for (int i = 0; i < bufferCount; i++) {
      totalSize += buffers[i].remaining();
    }

    ByteBuffer pipedBuffer = ByteBuffer.allocate(totalSize);
    for (int i = 0; i < bufferCount; i++) {
      pipedBuffer.put(buffers[i]);
    }

    pipedBuffer.flip();
    setBuffer(pipedBuffer);
  }

  private static ByteBuffer removePipeFromLastBuffer(ByteBuffer buffer) {
    byte[] bufferBytes = new byte[buffer.remaining()];
    buffer.mark();
    buffer.get(bufferBytes);
    buffer.reset();

    String command = new String(bufferBytes);
    String modifiedCommand = command.replaceFirst("\\s+pipe\\r\\n", "\r\n");
    byte[] modifiedBytes = modifiedCommand.getBytes();
    ByteBuffer newBuffer = ByteBuffer.allocate(modifiedBytes.length);
    newBuffer.put(modifiedBytes);
    newBuffer.flip();
    return newBuffer;
  }

  @Override
  public void handleLine(String line) {

    /* ENABLE_REPLICATION if */
    if (hasSwitchedOver(line)) {
      prepareSwitchover(line);
      return;
    }
    /* ENABLE_REPLICATION end */

    /* ENABLE_MIGRATION if */
    if (hasNotMyKey(line)) {
      String key = ops.get(responseIndex).getKeys().iterator().next();
      if (isBulkOperation()) {
        addRedirectMultiKeyOperation(line, key);
        responseIndex++;
      } else {
        // Only one NOT_MY_KEY is provided in response of
        // single key piped operation when redirection.
        addRedirectSingleKeyOperation(line, key);
        transitionState(OperationState.REDIRECT);
      }
      return;
    }
    /* ENABLE_MIGRATION end */

    /*
      RESPONSE <count>\r\n
      <status of the 1st pipelined command>\r\n
      [ ... ]
      <status of the last pipelined command>\r\n
      END|PIPE_ERROR <error_string>\r\n
    */
    if (line.startsWith("END")) {
      /* ENABLE_MIGRATION if */
      if (needRedirect()) {
        transitionState(OperationState.REDIRECT);
        return;
      }
      /* ENABLE_MIGRATION end */

      OperationStatus status = successAll ? END : FAILED_END;
      complete(status);
    } else if (line.startsWith("PIPE_ERROR")) {
      String errorMessage = line.substring(11);
      OperationStatus status =
          new OperationStatus(false, errorMessage, StatusCode.ERR_INTERNAL);
      complete(status);
    } else if (line.startsWith("RESPONSE ")) {
      expectingResponse = true;
      responseIndex = 0;
    } else if (expectingResponse) {
      // Handle status line for each command
      OperationStatus status = parseStatusLine(line);
      if (!status.isSuccess()) {
        successAll = false;
      }

      // Notify callback with current response index
      cb.gotStatus(ops.get(responseIndex), status);
      responseIndex++;
    } else {
      // Handle single command response (non-pipe case)
      // When only one command or last command without "pipe", server sends direct status
      OperationStatus status = parseStatusLine(line);
      if (!status.isSuccess()) {
        successAll = false;
      }

      // Notify callback for single command
      cb.gotStatus(ops.get(0), status);

      // Complete the operation immediately for single command
      complete(successAll ? END : FAILED_END);
    }
  }

  private OperationStatus parseStatusLine(String line) {
    return matchStatus(line,
        END, FAILED_END, CREATED_STORED, STORED, REPLACED, UPDATED,
        EXIST, NOT_EXIST, DELETED, DELETED_DROPPED, NOT_FOUND, NOT_FOUND_ELEMENT,
        NOTHING_TO_UPDATE, ELEMENT_EXISTS, OVERFLOWED, OUT_OF_RANGE,
        TYPE_MISMATCH, BKEY_MISMATCH, EFLAG_MISMATCH, UNREADABLE);
  }

  @Override
  protected void handleError(OperationErrorType eType, String line) throws IOException {
    // 1) Call the callback method to convert and save
    // into PipelineOperationException with index.
    exception = new OperationException(eType, line + " @ " + getHandlingNode().getNodeName());
    cb.gotStatus(ops.get(responseIndex),
        new OperationStatus(false, line, StatusCode.ERR_INTERNAL));

    // 2) Handle error for I/O Thread.
    if (!expectingResponse) {
      // this case means that error message came without 'RESPONSE <count>'.
      // so it doesn't need to read 'PIPE_ERROR'.
      super.handleError(eType, line);
    } else {
      // this case means that error message came after 'RESPONSE <count>'.
      // so it needs to read 'PIPE_ERROR'.
      getLogger().error("Error:  %s by %s", line, this);
    }
  }

  @Override
  public boolean isPipeOperation() {
    return ops.size() > 1;
  }

  @Override
  public boolean isBulkOperation() {
    return keys.size() > 1;
  }

  @Override
  public List<String> getKeys() {
    return keys;
  }

  @Override
  public List<KeyedOperation> getOps() {
    return ops;
  }
}
