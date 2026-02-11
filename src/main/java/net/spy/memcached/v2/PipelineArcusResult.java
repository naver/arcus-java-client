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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import net.spy.memcached.ops.OperationErrorType;
import net.spy.memcached.ops.OperationException;
import net.spy.memcached.ops.OperationStatus;
import net.spy.memcached.v2.pipe.PipelineOperationException;

/**
 * Custom result class for pipeline operations that supports indexed exceptions.
 */
public class PipelineArcusResult<T> implements ArcusResult<T> {

  protected AtomicReference<T> value;
  private final List<Exception> exceptions = new ArrayList<>();

  public PipelineArcusResult(AtomicReference<T> value) {
    this.value = value;
  }

  @Override
  public T get() {
    return value.get();
  }

  @Override
  public void set(T value) {
    this.value.set(value);
  }

  @Override
  public void addError(String key, OperationStatus status) {
    throw new UnsupportedOperationException(
        "Use another overloaded addError method for pipeline operations");
  }

  public void addError(int index, OperationStatus status) {
    exceptions.add(new PipelineOperationException(index,
        new OperationException(OperationErrorType.GENERAL,
            "{ message: " + status.getMessage() + " }")));
  }

  public void addError(int index, Throwable cause) {
    exceptions.add(new PipelineOperationException(index, cause));
  }

  @Override
  public boolean hasError() {
    return !exceptions.isEmpty();
  }

  @Override
  public List<Exception> getError() {
    return Collections.unmodifiableList(exceptions);
  }
}
