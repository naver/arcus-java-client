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
package net.spy.memcached.ops;

import java.util.ArrayList;
import java.util.Collection;

import net.spy.memcached.OperationFactory;

/**
 * Base class for operation factories.
 *
 * <p>
 * There is little common code between OperationFactory implementations, but
 * some exists, and is complicated and likely to cause problems.
 * </p>
 */
public abstract class BaseOperationFactory implements OperationFactory {

  public Collection<Operation> clone(KeyedOperation op) {
    assert op.getState() == OperationState.WRITE_QUEUED
            : "Who passed me an operation in the " + op.getState() + "state?";
    assert !op.isCancelled() : "Attempted to clone a canceled op";
    assert !op.hasErrored() : "Attempted to clone an errored op";

    Collection<Operation> rv = new ArrayList<Operation>(
            op.getKeys().size());
    if (op instanceof GetOperation) {
      GetOperation.Callback getCb = new MultiGetOperationCallback(
              op.getCallback(), op.getKeys().size());
      for (String k : op.getKeys()) {
        rv.add(get(k, getCb));
      }
    } else if (op instanceof GetsOperation) {
      GetsOperation.Callback getsCb = new MultiGetsOperationCallback(
              op.getCallback(), op.getKeys().size());
      for (String k : op.getKeys()) {
        rv.add(gets(k, getsCb));
      }
    } else {
      rv.add(op.clone());
    }
    return rv;
  }
}
