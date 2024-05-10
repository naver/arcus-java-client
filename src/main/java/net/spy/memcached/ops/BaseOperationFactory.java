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
import java.util.List;

import net.spy.memcached.MemcachedNode;
import net.spy.memcached.OperationFactory;
import net.spy.memcached.collection.BTreeGetBulk;
import net.spy.memcached.collection.BTreeSMGet;
import net.spy.memcached.collection.CollectionBulkInsert;

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
    assert op.getState() == OperationState.WRITE_QUEUED
            : "Who passed me an operation in the " + op.getState() + "state?";
    assert !op.isCancelled() : "Attempted to clone a canceled op";
    assert !op.hasErrored() : "Attempted to clone an errored op";

    Collection<Operation> rv = new ArrayList<>(
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
    } else if (op instanceof CollectionInsertOperation) {
      CollectionInsertOperation c = (CollectionInsertOperation) op;
      rv.add(collectionInsert(first(c.getKeys()), c.getSubKey(),
              c.getInsert(), c.getData(), c.getCallback()));
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

  @Override
  public MultiOperationCallback createMultiOperationCallback(KeyedOperation op, int count) {
    OperationCallback originalCallback = op.getCallback();
    if (op instanceof GetOperation) {
      return new MultiGetOperationCallback(originalCallback, count);
    } else if (op instanceof GetsOperation) {
      return new MultiGetsOperationCallback(originalCallback, count);
    } else if (op instanceof CollectionBulkInsertOperation) {
      return new MultiCollectionBulkInsertOperationCallback(originalCallback, count);
    } else if (op instanceof BTreeGetBulkOperation) {
      return new MultiBTreeGetBulkOperationCallback(originalCallback, count);
    } else if (op instanceof BTreeSortMergeGetOperation) {
      return new MultiBTreeSortMergeGetOperationCallback(originalCallback, count);
    } else {
      assert false : "Unhandled operation type: " + op.getClass();
    }
    return null;
  }

  @Override
  public Operation cloneMultiOperation(KeyedOperation op, MemcachedNode node,
                                       List<String> redirectKeys, MultiOperationCallback mcb) {
    assert !op.isCancelled() : "Attempted to clone a canceled op";
    assert !op.hasErrored() : "Attempted to clone an errored op";

    if (op instanceof GetOperation) {
      // If MemcachedNode supports this clone feature, it should support mget operation too.
      return mget(redirectKeys, (GetOperation.Callback) mcb);
    } else if (op instanceof GetsOperation) {
      // If MemcachedNode supports this clone feature, it should support mgets operation too.
      return mgets(redirectKeys, (GetsOperation.Callback) mcb);
    } else if (op instanceof CollectionBulkInsertOperation) {
      final CollectionBulkInsert<?> insert = ((CollectionBulkInsertOperation) op).getInsert();
      return collectionBulkInsert(insert.clone(node, redirectKeys), mcb);
    } else if (op instanceof BTreeGetBulkOperation) {
      final BTreeGetBulk<?> getbulk = ((BTreeGetBulkOperation) op).getBulk();
      return bopGetBulk(getbulk.clone(node, redirectKeys), (BTreeGetBulkOperation.Callback) mcb);
    } else if (op instanceof BTreeSortMergeGetOperation) {
      final BTreeSMGet<?> smGet = ((BTreeSortMergeGetOperation) op).getSMGet();
      return bopsmget(smGet.clone(node, redirectKeys), (BTreeSortMergeGetOperation.Callback) mcb);
    } else {
      assert false : "Unhandled operation type: " + op.getClass();
    }
    return null;
  }
}
