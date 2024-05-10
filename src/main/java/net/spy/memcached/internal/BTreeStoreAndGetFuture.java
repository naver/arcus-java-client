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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

import net.spy.memcached.OperationTimeoutException;
import net.spy.memcached.collection.Element;
import net.spy.memcached.internal.result.GetResult;

/**
 * Future object that contains an b+tree element object
 *
 * @param <T> Type of object returned from this future.
 * @param <E> the expected class of the value
 */
public class BTreeStoreAndGetFuture<T, E> extends CollectionFuture<T> {

  private GetResult<Element<E>> element;

  public BTreeStoreAndGetFuture(CountDownLatch l, long opTimeout) {
    this(l, new AtomicReference<>(null), opTimeout);
  }

  public BTreeStoreAndGetFuture(CountDownLatch l, AtomicReference<T> oref, long opTimeout) {
    super(l, oref, opTimeout);
  }

  public Element<E> getElement() {
    try {
      super.get(super.timeout, TimeUnit.MILLISECONDS);
    } catch (ExecutionException e) {
      throw new RuntimeException(e);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    } catch (TimeoutException e) {
      throw new OperationTimeoutException(e);
    }
    return element == null ? null : element.getDecodedValue();
  }

  public void setElement(GetResult<Element<E>> element) {
    this.element = element;
  }
}
