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
package net.spy.memcached;

import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

import net.spy.memcached.ops.Operation;

/**
 * Thrown by {@link MemcachedClient} when any internal operations timeout.
 *
 * @author Ray Krueger
 * @see net.spy.memcached.ConnectionFactory#getOperationTimeout()
 */
public class OperationTimeoutException extends RuntimeException {

  private static final long serialVersionUID = 1479557202445843619L;

  public OperationTimeoutException(String message) {
    super(message);
  }

  public OperationTimeoutException(Throwable cause) {
    super(cause);
  }

  public OperationTimeoutException(String message, Throwable cause) {
    super(message, cause);
  }

  public OperationTimeoutException(long duration,
                                   TimeUnit units,
                                   Operation op) {
    super(TimedOutMessageFactory
            .createTimedoutMessage(duration, units, Collections.singleton(op)));
  }

  public OperationTimeoutException(long duration,
                                   TimeUnit units,
                                   Collection<Operation> ops) {
    super(TimedOutMessageFactory
            .createTimedoutMessage(duration, units, ops));
  }
}
