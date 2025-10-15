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

import java.util.EventListener;
import java.util.concurrent.Future;

/**
 * A generic listener that will be notified once the future completes.
 *
 * <p>While this listener can be used directly, it is advised to subclass
 * it to make it easier for the API user to work with. </p>
 */
public interface GenericCompletionListener<T> extends EventListener {
  /**
   * This method will be executed once the future completes.
   *
   * <p>Completion includes both failure and success, so it is advised to
   * always check the status and errors of the future.</p>
   *
   * @param future the future that got completed.
   * @throws Exception can potentially throw anything in the callback.
   */
  void onComplete(Future<T> future) throws Exception;
}
