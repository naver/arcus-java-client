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
package net.spy.memcached.internal;

import java.util.Collection;
import java.util.concurrent.CountDownLatch;

import net.spy.memcached.ops.CollectionOperationStatus;
import net.spy.memcached.ops.Operation;
import net.spy.memcached.ops.OperationStatus;

public class CollectionGetBulkFuture<T> extends CollectionBulkFuture<T> {

  private final Collection<Operation> ops;

  public CollectionGetBulkFuture(CountDownLatch latch, Collection<Operation> ops, T result,
                                 long timeout) {
    super(ops, timeout, latch, result);
    this.ops = ops;
  }

  @Override
  public boolean isCancelled() {
    boolean rv = false;
    for (Operation op : ops) {
      rv |= op.isCancelled();
    }
    return rv;
  }

  public CollectionOperationStatus getOperationStatus() {
    if (isCancelled()) {
      return new CollectionOperationStatus(new OperationStatus(false, "CANCELED"));
    }
    return new CollectionOperationStatus(new OperationStatus(true, "END"));
  }
}
