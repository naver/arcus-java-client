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
import java.util.Collections;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import net.spy.memcached.ExceptionMessageFactory;
import net.spy.memcached.ops.Operation;

/**
 * Timeout exception that tracks the original operation.
 */
public class CheckedOperationTimeoutException extends TimeoutException {

  private static final long serialVersionUID = 5187393339735774489L;
  private final Collection<Operation> operations;

  public CheckedOperationTimeoutException(long duration,
                                          TimeUnit unit,
                                          long elapsed,
                                          Operation op) {
    this(duration, unit, elapsed, Collections.singleton(op));
  }

  public CheckedOperationTimeoutException(long duration,
                                          TimeUnit unit,
                                          long elapsed,
                                          Collection<Operation> ops) {
    super(ExceptionMessageFactory.createTimedOutMessage(duration, unit, elapsed, ops));
    operations = ops;
  }

  /**
   * Get the operation that timed out.
   */
  public Collection<Operation> getOperations() {
    return operations;
  }
}
