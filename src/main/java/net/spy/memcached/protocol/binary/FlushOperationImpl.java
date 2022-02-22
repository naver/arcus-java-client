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

import net.spy.memcached.ops.FlushOperation;
import net.spy.memcached.ops.OperationCallback;

class FlushOperationImpl extends OperationImpl implements FlushOperation {

  private static final int CMD = 8;
  private final int delay;

  public FlushOperationImpl(OperationCallback cb) {
    this(0, cb);
  }

  public FlushOperationImpl(int d, OperationCallback cb) {
    super(CMD, generateOpaque(), cb);
    delay = d;
  }

  @Override
  public void initialize() {
    prepareBuffer("", 0, EMPTY_BYTES, delay);
  }

  @Override
  public boolean isBulkOperation() {
    return false;
  }

  @Override
  public boolean isPipeOperation() {
    return false;
  }

  @Override
  public boolean isIdempotentOperation() {
    return false;
  }

}
