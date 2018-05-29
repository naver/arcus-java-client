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

  private String first(Collection<String> keys) {
    return keys.iterator().next();
  }

  public Collection<Operation> clone(KeyedOperation op) {
    assert op.getState() == OperationState.WRITING
            : "Who passed me an operation in the " + op.getState() + "state?";
    assert !op.isCancelled() : "Attempted to clone a canceled op";
    assert !op.hasErrored() : "Attempted to clone an errored op";

    Collection<Operation> rv = new ArrayList<Operation>(
            op.getKeys().size());
    if (op instanceof GetOperation) {
      rv.addAll(cloneGet(op));
    } else if (op instanceof GetsOperation) {
      GetsOperation.Callback callback =
              (GetsOperation.Callback) op.getCallback();
      for (String k : op.getKeys()) {
        rv.add(gets(k, callback));
      }
    } else if (op instanceof CASOperation) {
      CASOperation cop = (CASOperation) op;
      rv.add(cas(cop.getStoreType(), first(op.getKeys()),
              cop.getCasValue(), cop.getFlags(), cop.getExpiration(),
              cop.getBytes(), cop.getCallback()));
    } else if (op instanceof DeleteOperation) {
      rv.add(delete(first(op.getKeys()), op.getCallback()));
    } else if (op instanceof MutatorOperation) {
      MutatorOperation mo = (MutatorOperation) op;
      rv.add(mutate(mo.getType(), first(op.getKeys()),
              mo.getBy(), mo.getDefault(), mo.getExpiration(),
              op.getCallback()));
    } else if (op instanceof StoreOperation) {
      StoreOperation so = (StoreOperation) op;
      rv.add(store(so.getStoreType(), first(op.getKeys()), so.getFlags(),
              so.getExpiration(), so.getData(), op.getCallback()));
    } else if (op instanceof ConcatenationOperation) {
      ConcatenationOperation c = (ConcatenationOperation) op;
      rv.add(cat(c.getStoreType(), c.getCasValue(), first(op.getKeys()),
              c.getData(), c.getCallback()));
    } else if (op instanceof SetAttrOperation) {
      SetAttrOperation c = (SetAttrOperation) op;
      rv.add(setAttr(first(c.getKeys()), c.getAttributes(),
              c.getCallback()));
    } else if (op instanceof GetAttrOperation) {
      GetAttrOperation c = (GetAttrOperation) op;
      rv.add(getAttr(first(c.getKeys()),
              (GetAttrOperation.Callback) c.getCallback()));
    } else if (op instanceof CollectionStoreOperation) {
      CollectionStoreOperation c = (CollectionStoreOperation) op;
      rv.add(collectionStore(first(c.getKeys()), c.getSubKey(),
              c.getStore(), c.getData(), c.getCallback()));
    } else if (op instanceof CollectionGetOperation) {
      CollectionGetOperation c = (CollectionGetOperation) op;
      rv.add(collectionGet(first(c.getKeys()), c.getGet(),
              (CollectionGetOperation.Callback) c.getCallback()));
    } else if (op instanceof CollectionDeleteOperation) {
      CollectionDeleteOperation c = (CollectionDeleteOperation) op;
      rv.add(collectionDelete(first(c.getKeys()), c.getDelete(),
              c.getCallback()));
    } else if (op instanceof CollectionExistOperation) {
      CollectionExistOperation c = (CollectionExistOperation) op;
      rv.add(collectionExist(first(c.getKeys()), c.getSubKey(),
              c.getExist(), c.getCallback()));
    } else {
      assert false : "Unhandled operation type: " + op.getClass();
    }

    return rv;
  }

  protected abstract Collection<? extends Operation> cloneGet(
          KeyedOperation op);

}
