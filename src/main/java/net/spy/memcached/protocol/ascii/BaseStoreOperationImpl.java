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
package net.spy.memcached.protocol.ascii;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Collections;

import net.spy.memcached.KeyUtil;
import net.spy.memcached.ops.OperationCallback;
import net.spy.memcached.ops.OperationState;
import net.spy.memcached.ops.OperationStatus;
import net.spy.memcached.ops.OperationType;

/**
 * Base class for ascii store operations (add, set, replace, append, prepend).
 */
abstract class BaseStoreOperationImpl extends OperationImpl {

  private static final int OVERHEAD = 32;
  private static final OperationStatus STORED =
          new OperationStatus(true, "STORED");
  protected final String type;
  protected final String key;
  protected final int flags;
  protected final int exp;
  protected final byte[] data;

  public BaseStoreOperationImpl(String t, String k, int f, int e,
                                byte[] d, OperationCallback cb) {
    super(cb);
    type = t;
    key = k;
    flags = f;
    exp = e;
    data = d;
    setOperationType(OperationType.WRITE);
  }

  @Override
  public void handleLine(String line) {
    assert getState() == OperationState.READING
            : "Read ``" + line + "'' when in " + getState() + " state";
    /* ENABLE_REPLICATION if */
    if (line.equals("SWITCHOVER") || line.equals("REPL_SLAVE")) {
      receivedMoveOperations(line);
      return;
    }
    /* ENABLE_REPLICATION end */
    /* ENABLE_MIGRATION if */
    if (line.startsWith("NOT_MY_KEY ")) {
      receivedMigrateOperations(line, true);
      return;
    }
    /* ENABLE_MIGRATION end */
    getCallback().receivedStatus(matchStatus(line, STORED));
    transitionState(OperationState.COMPLETE);
  }

  @Override
  public void initialize() {
    ByteBuffer bb = ByteBuffer.allocate(data.length
            + KeyUtil.getKeyBytes(key).length + OVERHEAD);
    setArguments(bb, type, key, flags, exp, data.length);
    assert bb.remaining() >= data.length + 2
            : "Not enough room in buffer, need another "
            + (2 + data.length - bb.remaining());
    bb.put(data);
    bb.put(CRLF);
    bb.flip();
    setBuffer(bb);

    if (getLogger().isDebugEnabled()) {
      getLogger().debug("Request in ascii protocol: "
              + (new String(bb.array())).replace("\r\n", "\\r\\n"));
    }
  }

  @Override
  protected void wasCancelled() {
    // XXX:  Replace this comment with why I did this
    getCallback().receivedStatus(CANCELLED);
  }

  public Collection<String> getKeys() {
    return Collections.singleton(key);
  }

  public int getFlags() {
    return flags;
  }

  public int getExpiration() {
    return exp;
  }

  public byte[] getData() {
    return data;
  }
}
