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

package net.spy.memcached.protocol.ascii;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;

import net.spy.memcached.KeyUtil;
import net.spy.memcached.ops.Operation;
import net.spy.memcached.ops.OperationCallback;
import net.spy.memcached.ops.OperationErrorType;
import net.spy.memcached.ops.OperationState;
import net.spy.memcached.ops.OperationStatus;
import net.spy.memcached.ops.StatusCode;
import net.spy.memcached.protocol.BaseOperationImpl;

/**
 * Operations on a memcached connection.
 */
public abstract class OperationImpl extends BaseOperationImpl implements Operation {

  protected static final byte[] CRLF = {'\r', '\n'};
  protected static final String CHARSET = "UTF-8";

  protected final ByteArrayOutputStream byteBuffer = new ByteArrayOutputStream();
  protected OperationReadType readType = OperationReadType.LINE;
  protected boolean foundCr = false;

  protected OperationImpl() {
    super();
  }

  protected OperationImpl(OperationCallback cb) {
    super();
    callback = cb;
  }

  /**
   * Match the status line provided against one of the given
   * OperationStatus objects.  If none match, return a failure status with
   * the given line.
   *
   * @param line   the current line
   * @param statii several status objects
   * @return the appropriate status object
   */
  protected final OperationStatus matchStatus(String line,
                                              OperationStatus... statii) {
    OperationStatus rv = null;
    for (OperationStatus status : statii) {
      if (line.equals(status.getMessage())) {
        rv = status;
      }
    }
    if (rv == null) {
      rv = new OperationStatus(false, line, StatusCode.fromAsciiLine(line));
    }
    return rv;
  }

  protected final OperationReadType getReadType() {
    return readType;
  }

  /**
   * Set the read type of this operation.
   */
  protected final void setReadType(OperationReadType to) {
    readType = to;
  }

  /**
   * Set some arguments for an operation into the given byte buffer.
   */
  protected final void setArguments(ByteBuffer bb, Object... args) {
    boolean wasFirst = true;
    for (Object o : args) {
      String s = String.valueOf(o);
      if (wasFirst) {
        wasFirst = false;
      } else if (!"".equals(s)) {
        bb.put((byte) ' ');
      }
      bb.put(KeyUtil.getKeyBytes(s));
    }
    bb.put(CRLF);
  }

  protected String getLineFromBuffer(ByteBuffer data) throws UnsupportedEncodingException {
    boolean lineFound = false;
    while (data.remaining() > 0) {
      byte b = data.get();
      if (b == '\r') {
        foundCr = true;
      } else if (b == '\n') {
        assert foundCr : "got a \\n without a \\r";
        foundCr = false;
        lineFound = true;
        break;
      } else {
        assert !foundCr : "got a \\r without a \\n";
        byteBuffer.write(b);
      }
    }
    if (lineFound) {
      String line = byteBuffer.toString(CHARSET);
      byteBuffer.reset();
      return line;
    }
    return null;
  }

  protected OperationErrorType classifyError(String line) {
    OperationErrorType rv = null;
    if (line.startsWith("ERROR")) {
      rv = OperationErrorType.GENERAL;
    } else if (line.startsWith("CLIENT_ERROR")) {
      rv = OperationErrorType.CLIENT;
    } else if (line.startsWith("SERVER_ERROR")) {
      rv = OperationErrorType.SERVER;
    }
    return rv;
  }

  @Override
  public void readFromBuffer(ByteBuffer data) throws IOException {
    // Loop while there's data remaining to get it all drained.
    while (data.remaining() > 0) {
      if (getState() == OperationState.COMPLETE ||
          getState() == OperationState.MOVING || // ENABLE_REPLICATION
          getState() == OperationState.REDIRECT) { // ENABLE_MIGRATION
        return;
      }
      if (readType == OperationReadType.LINE) {
        String line = getLineFromBuffer(data);
        if (line == null) {
          continue;
        }
        OperationErrorType eType = classifyError(line);
        if (eType != null) {
          handleError(eType, line);
        } else {
          handleLine(line);
        }
      } else { // OperationReadType.DATA
        handleRead(data);
      }
    }
  }

  public abstract void handleLine(String line);

  protected boolean hasSwitchedOver(String line) {
    return line.startsWith("SWITCHOVER") || line.startsWith("REPL_SLAVE");
  }

  /* ENABLE_MIGRATION if */
  protected boolean hasNotMyKey(String line) {
    return line.startsWith("NOT_MY_KEY");
  }

  protected String getNotMyKey(String line) {
    return line.substring(line.indexOf("NOT_MY_KEY"));
  }
  /* ENABLE_MIGRATION end */
}
