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
package net.spy.memcached.ops;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * An operation callback that will capture receivedStatus and complete
 * invocations and dispatch to a single callback.
 *
 * <p>
 * This is useful for the cases where a single request gets split into
 * multiple requests and the callback needs to not know the difference.
 * </p>
 */
public abstract class MultiOperationCallback implements OperationCallback {
  protected final OperationCallback originalCallback;
  private final AtomicInteger remaining;
  private OperationStatus mostRecentStatus = null;

  /**
   * Get a MultiOperationCallback over the given callback for the specified
   * number of replicates.
   *
   * @param original the original callback
   * @param todo     how many complete() calls we expect before dispatching.
   */
  public MultiOperationCallback(OperationCallback original, int todo) {
    originalCallback = original;
    remaining = new AtomicInteger(todo);
  }

  @Override
  public void complete() {
    if (remaining.decrementAndGet() == 0) {
      originalCallback.receivedStatus(mostRecentStatus);
      originalCallback.complete();
    }
  }

  @Override
  public void receivedStatus(OperationStatus status) {
    if (mostRecentStatus == null || !status.isSuccess()) {
      mostRecentStatus = status;
    }
  }

}
