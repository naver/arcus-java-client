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
package net.spy.memcached.plugin;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import net.spy.memcached.internal.GetFuture;
import net.spy.memcached.ops.OperationStatus;

import static net.spy.memcached.DefaultConnectionFactory.DEFAULT_OPERATION_TIMEOUT;

/**
 * Future returned for GET operations.
 *
 * Not intended for general use.
 *
 * @param <T> Type of object returned from the get
 */
public class FrontCacheGetFuture<T> extends GetFuture<T> {
  private final GetFuture<T> parent;
  private final LocalCacheManager localCacheManager;
  private final String key;

  public FrontCacheGetFuture(LocalCacheManager localCacheManager, String key, GetFuture<T> parent) {
    super(new CountDownLatch(0), DEFAULT_OPERATION_TIMEOUT);
    this.parent = parent;
    this.localCacheManager = localCacheManager;
    this.key = key;
  }

  @Override
  public T get(long timeout, TimeUnit unit) throws InterruptedException,
          ExecutionException, TimeoutException {
    T t = parent.get(timeout, unit);
    localCacheManager.put(key, t);
    return t;
  }

  @Override
  public OperationStatus getStatus() {
    return parent.getStatus();
  }

  @Override
  public boolean cancel(boolean ign) {
    return parent.cancel(ign);
  }

  @Override
  public boolean isDone() {
    return parent.isDone();
  }

  @Override
  public boolean isCancelled() {
    return parent.isCancelled();
  }
}
