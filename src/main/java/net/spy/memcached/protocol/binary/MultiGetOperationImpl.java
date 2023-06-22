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
package net.spy.memcached.protocol.binary;

import java.io.IOException;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import net.spy.memcached.KeyUtil;
import net.spy.memcached.ops.GetOperation;
import net.spy.memcached.ops.OperationCallback;
import net.spy.memcached.ops.OperationState;

import static net.spy.memcached.protocol.binary.GetOperationImpl.EXTRA_HDR_LEN;

class MultiGetOperationImpl extends OperationImpl implements GetOperation {

  private static final int CMD_GETQ = 9;

  private final Map<Integer, String> keys = new HashMap<Integer, String>();
  private final Map<Integer, byte[]> bkeys = new HashMap<Integer, byte[]>();
  private final Map<String, Integer> rkeys = new HashMap<String, Integer>();

  private final int terminalOpaque = generateOpaque();

  public MultiGetOperationImpl(Collection<String> k, OperationCallback cb) {
    super(-1, -1, cb);
    for (String s : new HashSet<String>(k)) {
      addKey(s);
    }
  }

  /**
   * Add a key (and return its new opaque value).
   */
  protected int addKey(String k) {
    Integer rv = rkeys.get(k);
    if (rv == null) {
      rv = generateOpaque();
      keys.put(rv, k);
      bkeys.put(rv, KeyUtil.getKeyBytes(k));
      rkeys.put(k, rv);
    }
    return rv;
  }

  @Override
  public void initialize() {
    int size = (1 + keys.size()) * MIN_RECV_PACKET;
    for (byte[] b : bkeys.values()) {
      size += b.length;
    }
    // set up the initial header stuff
    ByteBuffer bb = ByteBuffer.allocate(size);
    for (Map.Entry<Integer, byte[]> me : bkeys.entrySet()) {
      final byte[] keyBytes = me.getValue();

      // Custom header
      bb.put(REQ_MAGIC);
      bb.put((byte) CMD_GETQ);
      bb.putShort((short) keyBytes.length);
      bb.put((byte) 0); // extralen
      bb.put((byte) 0); // data type
      bb.putShort((short) 0); // reserved
      bb.putInt(keyBytes.length);
      bb.putInt(me.getKey());
      bb.putLong(0); // cas
      // the actual key
      bb.put(keyBytes);
    }
    // Add the noop
    bb.put(REQ_MAGIC);
    bb.put((byte) NoopOperationImpl.CMD);
    bb.putShort((short) 0);
    bb.put((byte) 0); // extralen
    bb.put((byte) 0); // data type
    bb.putShort((short) 0); // reserved
    bb.putInt(0);
    bb.putInt(terminalOpaque);
    bb.putLong(0); // cas

    ((Buffer) bb).flip();
    setBuffer(bb);
  }

  @Override
  protected void finishedPayload(byte[] pl) throws IOException {
    if (responseOpaque == terminalOpaque) {
      getCallback().receivedStatus(STATUS_OK);
      transitionState(OperationState.COMPLETE);
    } else if (errorCode != 0) {
      getLogger().warn("Error on key %s:  %s (%d)",
              keys.get(responseOpaque), new String(pl), errorCode);
    } else {
      final int flags = decodeInt(pl, 0);
      final byte[] data = new byte[pl.length - EXTRA_HDR_LEN];
      System.arraycopy(pl, EXTRA_HDR_LEN, data,
              0, pl.length - EXTRA_HDR_LEN);
      Callback cb = (Callback) getCallback();
      cb.gotData(keys.get(responseOpaque), flags, data);
    }
    resetInput();
  }

  @Override
  protected boolean opaqueIsValid() {
    return responseOpaque == terminalOpaque
            || keys.containsKey(responseOpaque);
  }

  public Collection<String> getKeys() {
    return keys.values();
  }

  @Override
  public boolean isBulkOperation() {
    return true;
  }

  @Override
  public boolean isPipeOperation() {
    return false;
  }

  @Override
  public boolean isIdempotentOperation() {
    return true;
  }

}
