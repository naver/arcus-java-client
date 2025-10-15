/*
 * arcus-java-client : Arcus Java client
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
package net.spy.memcached.internal;

import java.util.concurrent.Future;

/**
 * A {@link Future} that accepts one or more listeners that will be executed
 * asynchronously.
 */
public interface ListenableFuture<T, L> extends Future<T> {
  /**
   * Add a listener to the future, which will be executed once the operation
   * completes.
   *
   * @param listener the listener which will be executed.
   * @return the current future to allow for object-chaining.
   */
  Future<T> addListener(GenericCompletionListener<T> listener);

  /**
   * Remove a previously added listener from the future.
   *
   * @param listener the previously added listener.
   * @return the current future to allow for object-chaining.
   */
  Future<T> removeListener(GenericCompletionListener<T> listener);
}
