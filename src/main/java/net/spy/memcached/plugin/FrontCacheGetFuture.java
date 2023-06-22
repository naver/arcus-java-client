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

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import net.spy.memcached.internal.GetFuture;
import net.spy.memcached.ops.OperationStatus;
import net.spy.memcached.ops.StatusCode;

import net.sf.ehcache.Element;

/**
 * Future returned for GET operations.
 *
 * Not intended for general use.
 *
 * @param <T> Type of object returned from the get
 */
public class FrontCacheGetFuture<T> extends GetFuture<T> {

  private static final OperationStatus END =
          new OperationStatus(true, "END", StatusCode.SUCCESS);
  private final Element element;


  public FrontCacheGetFuture(Element element) {
    super(null, 0);
    this.element = element;
  }

  @Override
  public boolean cancel(boolean mayInterruptIfRunning) {
    return false;
  }

  @Override
  public T get() throws InterruptedException, ExecutionException {
    return getValue();
  }

  @Override
  public OperationStatus getStatus() {
    return END;
  }

  @SuppressWarnings("unchecked")
  private T getValue() {
    return (T) this.element.getObjectValue();
  }

  @Override
  public T get(long timeout, TimeUnit unit) throws InterruptedException,
          ExecutionException, TimeoutException {
    return getValue();
  }

  @Override
  public boolean isCancelled() {
    return false;
  }

  @Override
  public boolean isDone() {
    return true;
  }

}
