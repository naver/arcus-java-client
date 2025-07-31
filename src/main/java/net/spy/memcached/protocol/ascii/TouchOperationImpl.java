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

import net.spy.memcached.KeyUtil;
import net.spy.memcached.ops.OperationCallback;
import net.spy.memcached.ops.OperationState;
import net.spy.memcached.ops.OperationStatus;
import net.spy.memcached.ops.StatusCode;
import net.spy.memcached.ops.TouchOperation;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Collections;

/**
 * Operation to touch the expiration time from a memcached server.
 */
final class TouchOperationImpl extends OperationImpl implements TouchOperation {

  private static final int OVERHEAD = 9;

  private static final OperationStatus OK = new OperationStatus(true, "TOUCHED",
          StatusCode.SUCCESS);
  private static final OperationStatus NOT_FOUND = new OperationStatus(false, "NOT_FOUND",
          StatusCode.ERR_NOT_FOUND);

  private final String key;
  private final int exp;

  public TouchOperationImpl(String k, int t, OperationCallback cb) {
    super(cb);
    key = k;
    exp = t;
  }

  public Collection<String> getKeys() {
    return Collections.singleton(key);
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
      addRedirectSingleKeyOperation(line, key);
      transitionState(OperationState.REDIRECT);
      return;
    }
    /* ENABLE_MIGRATION end */

    complete(matchStatus(line, OK, NOT_FOUND));
  }

  @Override
  public void initialize() {
    ByteBuffer bb = ByteBuffer.allocate(KeyUtil.getKeyBytes(key).length
            + String.valueOf(exp).length() + OVERHEAD);
    setArguments(bb, "touch", key, String.valueOf(exp));
    bb.flip();
    setBuffer(bb);
  }

  @Override
  public int getExpiration() {
    return exp;
  }

  @Override
  public String toString() {
    return "Cmd: touch key: " + key + " exp: " + exp;
  }
}
