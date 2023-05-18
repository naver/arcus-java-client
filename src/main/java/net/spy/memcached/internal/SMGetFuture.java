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
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Collections;
import java.util.ArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.CountDownLatch;

import net.spy.memcached.MemcachedConnection;
import net.spy.memcached.collection.SMGetTrimKey;
import net.spy.memcached.ops.CollectionOperationStatus;
import net.spy.memcached.ops.Operation;
import net.spy.memcached.ops.OperationStatus;

import static net.spy.memcached.ArcusClient.bulkOpTimeOutHandler;

public class SMGetFuture<T> extends CollectionBulkFuture<T> {

  private final Collection<Operation> ops;
  private final long timeout;

  private final CountDownLatch broadcastLatch;

  private final int smGetListSize;

  private final T mergedResult;

  private final T subListResult;

  private final Map<String, CollectionOperationStatus> missedKeys;

  private final List<SMGetTrimKey> mergedTrimKeys;

  private final List<OperationStatus> failedOperationStatus;

  private final List<OperationStatus> resultOperationStatus;


  public SMGetFuture(Collection<Operation> ops, long timeout,
                     CountDownLatch broadcastLatch,
                     int smGetListSize,
                     T mergedResult, T subListResult,
                     Map<String, CollectionOperationStatus> missedKeys,
                     List<SMGetTrimKey> mergedTrimKeys,
                     List<OperationStatus> failedOperationStatus,
                     List<OperationStatus> resultOperationStatus) {
    super(ops, timeout, broadcastLatch, mergedResult);
    this.ops = ops;
    this.timeout = timeout;
    this.broadcastLatch = broadcastLatch;
    this.smGetListSize = smGetListSize;
    this.mergedResult = mergedResult;
    this.subListResult = subListResult;
    this.missedKeys = missedKeys;
    this.mergedTrimKeys = mergedTrimKeys;
    this.failedOperationStatus = failedOperationStatus;
    this.resultOperationStatus = resultOperationStatus;
  }

  @Override
  public T get(long duration, TimeUnit units)
          throws InterruptedException, TimeoutException, ExecutionException {

    if (!broadcastLatch.await(duration, units)) {
      bulkOpTimeOutHandler(duration, units, ops);
    } else {
      // continuous timeout counter will be reset
      MemcachedConnection.opsSucceeded(ops);
    }

    for (Operation op : ops) {
      if (op != null && op.hasErrored()) {
        throw new ExecutionException(op.getException());
      }

      if (op != null && op.isCancelled()) {
        throw new ExecutionException(new RuntimeException(op.getCancelCause()));
      }
    }

    if (smGetListSize == 1) {
      return mergedResult;
    }

    return subListResult;
  }

  @Override
  public boolean isCancelled() {
    boolean rv = false;
    for (Operation op : ops) {
      rv |= op.isCancelled();
    }
    return rv;
  }

  public Map<String, CollectionOperationStatus> getMissedKeys() {
    return missedKeys;
  }

  public List<String> getMissedKeyList() {
    Set<String> keyList = missedKeys.keySet();
    return Collections.synchronizedList(new ArrayList<String>(keyList));
  }

  public List<SMGetTrimKey> getTrimmedKeys() {
    return mergedTrimKeys;
  }

  public CollectionOperationStatus getOperationStatus() {
    if (failedOperationStatus.size() > 0) {
      return new CollectionOperationStatus(failedOperationStatus.get(0));
    }
    return new CollectionOperationStatus(resultOperationStatus.get(0));
  }

}
