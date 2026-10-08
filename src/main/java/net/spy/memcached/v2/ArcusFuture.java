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
package net.spy.memcached.v2;

import java.util.concurrent.CompletionStage;
import java.util.concurrent.Future;

/**
 * Result of an asynchronous Arcus operation.
 *
 * <p>Each Arcus operation has its own timeout, measured from submission until the complete
 * response arrives. Decoding and dependent stages are not included. On timeout, the operation
 * is cancelled and this future completes exceptionally with
 * {@link net.spy.memcached.OperationTimeoutException}. For a future that combines several
 * operations, the total time may exceed the timeout.</p>
 *
 * <p>Non-async dependent stages may run on an Arcus internal thread and must not block.</p>
 *
 * <p><b>Do not complete the future returned by {@link #toCompletableFuture()} directly;</b>
 * it does not cancel the Arcus operation.</p>
 *
 * @param <T> operation result type
 */
public interface ArcusFuture<T> extends CompletionStage<T>, Future<T> {

  /**
   * Internal completion callback. Not for application use.
   */
  void complete();

}
