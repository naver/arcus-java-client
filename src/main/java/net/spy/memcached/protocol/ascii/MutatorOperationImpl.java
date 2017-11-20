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

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Collections;

import net.spy.memcached.KeyUtil;
import net.spy.memcached.ops.APIType;
import net.spy.memcached.ops.MutatorOperation;
import net.spy.memcached.ops.Mutator;
import net.spy.memcached.ops.OperationCallback;
import net.spy.memcached.ops.OperationState;
import net.spy.memcached.ops.OperationStatus;
import net.spy.memcached.ops.OperationType;

/**
 * Operation for mutating integers inside of memcached.
 */
final class MutatorOperationImpl extends OperationImpl
        implements MutatorOperation {

  public static final int OVERHEAD = 32;

  private static final OperationStatus NOT_FOUND =
          new OperationStatus(false, "NOT_FOUND");
  private static final OperationStatus TYPE_MISMATCH =
          new OperationStatus(false, "TYPE_MISMATCH");

  private final Mutator mutator;
  private final String key;
  private final int amount;
  private final long def;
  private final int exp;

  public MutatorOperationImpl(Mutator m, String k, int amt, long d, int e,
                              OperationCallback c) {
    super(c);
    mutator = m;
    key = k;
    amount = amt;
    def = d;
    exp = e;
    if (m == Mutator.incr)
      setAPIType(APIType.INCR);
    else if (m == Mutator.decr)
      setAPIType(APIType.DECR);
    setOperationType(OperationType.WRITE);
  }

  @Override
  public void handleLine(String line) {
    /* ENABLE_REPLICATION if */
    if (line.equals("SWITCHOVER") || line.equals("REPL_SLAVE")) {
      receivedMoveOperations(line);
    }

    /* ENABLE_REPLICATION end */
    /* ENABLE_MIGRATION if */
    if (line.startsWith("NOT_MY_KEY ")) {
      receivedMigrateOperations(line, true);
      return;
    }
    /* ENABLE_MIGRATION end */
    OperationStatus status = null;
    try {
      Long.valueOf(line);
      getCallback().receivedStatus(new OperationStatus(true, line));
    } catch (NumberFormatException e) {
      status = matchStatus(line, NOT_FOUND, TYPE_MISMATCH);
      getCallback().receivedStatus(status);
    }
    transitionState(OperationState.COMPLETE);
  }

  @Override
  public void initialize() {
    int size = KeyUtil.getKeyBytes(key).length + OVERHEAD;
    ByteBuffer b = ByteBuffer.allocate(size);
    if (def > -1) {
      setArguments(b, mutator.name(), key, amount, 0, exp, def);
    } else {
      setArguments(b, mutator.name(), key, amount);
    }
    b.flip();
    setBuffer(b);
  }

  @Override
  protected void wasCancelled() {
    // XXX:  Replace this comment with why the hell I did this.
    getCallback().receivedStatus(CANCELLED);
  }

  public Collection<String> getKeys() {
    return Collections.singleton(key);
  }

  public int getBy() {
    return amount;
  }

  public long getDefault() {
    return -1;
  }

  public int getExpiration() {
    return -1;
  }

  public Mutator getType() {
    return mutator;
  }

}
