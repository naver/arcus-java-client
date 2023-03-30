/*
 * arcus-java-client : Arcus Java client
 * Copyright 2010-2014 NAVER Corp.
 * Copyright 2014-2021 JaM2in Co., Ltd.
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
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;

import net.spy.memcached.MemcachedNode;
import net.spy.memcached.MemcachedReplicaGroup;
import net.spy.memcached.RedirectHandler;
import net.spy.memcached.compat.SpyObject;
import net.spy.memcached.ops.CancelledOperationStatus;
import net.spy.memcached.ops.OperationException;
import net.spy.memcached.ops.OperationState;
import net.spy.memcached.ops.OperationStatus;
import net.spy.memcached.ops.OperationCallback;
import net.spy.memcached.ops.OperationType;
import net.spy.memcached.ops.OperationErrorType;
import net.spy.memcached.ops.APIType;
import net.spy.memcached.ops.StatusCode;

/**
 * Base class for protocol-specific operation implementations.
 */
public abstract class BaseOperationImpl extends SpyObject {

  /**
   * Status object for canceled operations.
   */
  public static final OperationStatus CANCELLED =
          new CancelledOperationStatus();
  private OperationState state = OperationState.WRITE_QUEUED;
  private ByteBuffer cmd = null;
  private String cmdString = null;
  private final boolean isAscii;
  private boolean cancelled = false;
  private String cancelCause = null;
  private OperationException exception = null;
  protected OperationCallback callback = null;
  private volatile MemcachedNode handlingNode = null;

  private OperationType opType = OperationType.UNDEFINED;
  private APIType apiType = APIType.UNDEFINED;

  /* ENABLE_MIGRATION if */
  private RedirectHandler redirectHandler = null;
  /* ENABLE_MIGRATION end */

  public BaseOperationImpl(boolean isAscii) {
    super();
    this.isAscii = isAscii;
  }

  /**
   * Get the operation callback associated with this operation.
   */
  public final OperationCallback getCallback() {
    return callback;
  }

  /**
   * Set the callback for this instance.
   */
  protected void setCallback(OperationCallback to) {
    callback = to;
  }

  public final boolean isCancelled() {
    return cancelled;
  }

  public final boolean hasErrored() {
    return exception != null;
  }

  public final OperationException getException() {
    return exception;
  }

  public final void cancel(String cause) {
    cancelled = true;
    if (handlingNode != null) {
      cancelCause = "Cancelled (" + cause + " : (" + handlingNode.getNodeName() + ")" + ")";
    } else {
      cancelCause = "Cancelled (" + cause + ")";
    }
    wasCancelled();
    callback.complete();
  }

  public final String getCancelCause() {
    return cancelCause;
  }

  /**
   * This is called on each subclass whenever an operation was cancelled.
   */
  protected void wasCancelled() {
    getLogger().debug("was cancelled.");
  }

  public final OperationState getState() {
    return state;
  }

  /* ENABLE_REPLICATION if */
  /**
   * reset operation state to WRITE_QUEUED
   */
  @SuppressWarnings("fallthrough")
  public final void reset() {
    switch (getState()) {
      case WRITING:
        transitionState(OperationState.WRITE_QUEUED); // reset operation state
        // fallthrough
      case WRITE_QUEUED:
        if (getBuffer() != null) {
          ((Buffer) getBuffer()).reset(); // buffer offset reset
        } else {
          initialize(); // this case cannot happen.
        }
        break;
      default:
        initialize(); // write completed or not yet initialized
        transitionState(OperationState.WRITE_QUEUED); // reset operation state
    }
  }

  protected final void receivedMoveOperations(String cause) {
    // switchover message e.g.
    // one slave node case : "SWITCHOVER", "REPL_SLAVE",
    // two or more than slave nodes case : "SWITCHOVER <ip:port>", "REPL_SLAVE <ip:port>"
    String[] messages = cause.split(" ");
    MemcachedReplicaGroup group = handlingNode.getReplicaGroup();
    if (messages.length == 1) {
      group.setMasterCandidate();
    } else if (messages.length == 2) {
      group.setMasterCandidateByAddr(messages[1]);
    }

    if (group.getMasterCandidate() == null) {
      getLogger().error("there is a problem to set master candidate : %s by %s from %s",
              cause, this, handlingNode);
      getCallback().receivedStatus(new OperationStatus(false, cause, StatusCode.UNDEFINED));
      transitionState(OperationState.COMPLETE);
      return;
    }

    getLogger().info("%s message received by %s operation from %s", cause, this, handlingNode);
    transitionState(OperationState.MOVING);
  }
  /* ENABLE_REPLICATION end */

  /* ENABLE_MIGRATION if */
  public final RedirectHandler getAndClearRedirectHandler() {
    RedirectHandler redirectHandler = this.redirectHandler;
    this.redirectHandler = null;

    return redirectHandler;
  }

  protected final void addRedirectSingleKeyOperation(String response, String key) {
    if (redirectHandler == null) {
      redirectHandler = new RedirectHandler.RedirectHandlerSingleKey();
    }
    redirectHandler.addRedirectKey(response, key);
  }

  protected final void addRedirectMultiKeyOperation(String response, String key) {
    if (redirectHandler == null) {
      redirectHandler = new RedirectHandler.RedirectHandlerMultiKey();
    }
    redirectHandler.addRedirectKey(response, key);
  }

  protected final boolean needRedirect() {
    return redirectHandler != null;
  }
  /* ENABLE_MIGRATION end */

  public final ByteBuffer getBuffer() {
    return cmd;
  }

  /**
   * Set the write buffer for this operation.
   */
  protected final void setBuffer(ByteBuffer to) {
    assert to != null : "Trying to set buffer to null";
    cmd = to;
    ((Buffer) cmd).mark();

    if (getLogger().isDebugEnabled()) {
      if (cmdString == null) {
        cmdString = generateCmdString();
      }
      if (isAscii) {
        getLogger().debug("Request in ascii protocol: %s", cmdString);
      }
    }
  }

  private String generateCmdString() {
    String result = null;
    if (isAscii) {
      String temp = new String(cmd.array(), Charset.forName("UTF-8")).trim();
      String[] lines = temp.split("\r\n");

      if (lines.length > 0) {
        result = lines[0].trim();

        if (lines.length > 1 && !hasValue()) {
          result += "\\r\\n" + lines[1].trim();
        }
      }
    } else {
      result = "Binary" + this.getClass().getSimpleName();
    }
    return result;
  }

  /**
   * Transition the state of this operation to the given state.
   */
  protected final void transitionState(OperationState newState) {
    getLogger().debug("Transitioned state from %s to %s, op=%s, node=%s",
        state,
        newState,
        this,
        getHandlingNode());

    state = newState;
    // Discard our buffer when we no longer need it.
    if (state != OperationState.WRITE_QUEUED &&
        state != OperationState.WRITING) {
      cmd = null;
    }
    if (state == OperationState.COMPLETE) {
      callback.complete();
    }
  }

  public final void writing() {
    transitionState(OperationState.WRITING);
  }

  public final void writeComplete() {
    transitionState(OperationState.READING);
  }

  public abstract void initialize();

  public abstract void readFromBuffer(ByteBuffer data) throws IOException;

  protected void handleError(OperationErrorType eType, String line)
          throws IOException {
    getLogger().error("Error:  %s by %s", line, this);
    switch (eType) {
      case GENERAL:
        exception = new OperationException();
        break;
      case SERVER:
        exception = new OperationException(eType, line);
        break;
      case CLIENT:
        if (line.contains("bad command line format")) {
          initialize();
          byte[] bytes = new byte[cmd.remaining()];
          cmd.get(bytes);

          String[] cmdLines = new String(bytes).split("\r\n");
          getLogger().error("Bad command: %s", cmdLines[0]);
        }
        exception = new OperationException(eType, line);
        break;
      default:
        assert false;
    }
    transitionState(OperationState.COMPLETE);
    throw exception;
  }

  public void handleRead(ByteBuffer data) {
    assert false;
  }

  public MemcachedNode getHandlingNode() {
    return handlingNode;
  }

  public void setHandlingNode(MemcachedNode to) {
    handlingNode = to;
  }

  public OperationType getOperationType() {
    return opType;
  }

  public void setOperationType(OperationType opType) {
    this.opType = opType;
  }

  public boolean isWriteOperation() {
    return this.opType == OperationType.WRITE;
  }

  public boolean isReadOperation() {
    return this.opType == OperationType.READ;
  }

  public APIType getAPIType() {
    return this.apiType;
  }

  public void setAPIType(APIType type) {
    this.apiType = type;
  }

  public abstract boolean isBulkOperation();

  public abstract boolean isPipeOperation();

  protected abstract boolean hasValue();

  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder();
    builder.append(this.getClass().getSimpleName()).append("{")
        .append("hashCode=").append(hashCode())
        .append(", state=").append(getState());

    if (cmdString != null) {
      builder.append(", cmd=").append(cmdString);
    }

    builder.append("}");
    return builder.toString();
  }
}
