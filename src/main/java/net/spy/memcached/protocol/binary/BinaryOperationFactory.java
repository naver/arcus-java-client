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
package net.spy.memcached.protocol.binary;

import java.util.Collection;

import javax.security.sasl.SaslClient;

import net.spy.memcached.collection.Attributes;
import net.spy.memcached.collection.BTreeFindPosition;
import net.spy.memcached.collection.BTreeFindPositionWithGet;
import net.spy.memcached.collection.BTreeGetBulk;
import net.spy.memcached.collection.BTreeGetByPosition;
import net.spy.memcached.collection.BTreeInsertAndGet;
import net.spy.memcached.collection.BTreeSMGet;
import net.spy.memcached.collection.CollectionBulkInsert;
import net.spy.memcached.collection.CollectionCount;
import net.spy.memcached.collection.CollectionCreate;
import net.spy.memcached.collection.CollectionDelete;
import net.spy.memcached.collection.CollectionExist;
import net.spy.memcached.collection.CollectionGet;
import net.spy.memcached.collection.CollectionInsert;
import net.spy.memcached.collection.CollectionMutate;
import net.spy.memcached.collection.CollectionPipedInsert;
import net.spy.memcached.collection.CollectionPipedUpdate;
import net.spy.memcached.collection.CollectionUpdate;
import net.spy.memcached.collection.SetPipedExist;
import net.spy.memcached.ops.BTreeFindPositionOperation;
import net.spy.memcached.ops.BTreeFindPositionWithGetOperation;
import net.spy.memcached.ops.BTreeGetBulkOperation;
import net.spy.memcached.ops.BTreeGetByPositionOperation;
import net.spy.memcached.ops.BTreeInsertAndGetOperation;
import net.spy.memcached.ops.BTreeSortMergeGetOperation;
import net.spy.memcached.ops.BTreeSortMergeGetOperationOld;
import net.spy.memcached.ops.BaseOperationFactory;
import net.spy.memcached.ops.CASOperation;
import net.spy.memcached.ops.CollectionBulkInsertOperation;
import net.spy.memcached.ops.CollectionCountOperation;
import net.spy.memcached.ops.CollectionCreateOperation;
import net.spy.memcached.ops.CollectionDeleteOperation;
import net.spy.memcached.ops.CollectionExistOperation;
import net.spy.memcached.ops.CollectionGetOperation;
import net.spy.memcached.ops.CollectionInsertOperation;
import net.spy.memcached.ops.CollectionMutateOperation;
import net.spy.memcached.ops.CollectionPipedExistOperation;
import net.spy.memcached.ops.CollectionPipedInsertOperation;
import net.spy.memcached.ops.CollectionPipedUpdateOperation;
import net.spy.memcached.ops.CollectionUpdateOperation;
import net.spy.memcached.ops.ConcatenationOperation;
import net.spy.memcached.ops.ConcatenationType;
import net.spy.memcached.ops.DeleteOperation;
import net.spy.memcached.ops.FlushOperation;
import net.spy.memcached.ops.GetAttrOperation;
import net.spy.memcached.ops.GetOperation;
import net.spy.memcached.ops.GetOperation.Callback;
import net.spy.memcached.ops.GetsOperation;
import net.spy.memcached.ops.Mutator;
import net.spy.memcached.ops.MutatorOperation;
import net.spy.memcached.ops.NoopOperation;
import net.spy.memcached.ops.OperationCallback;
import net.spy.memcached.ops.SASLAuthOperation;
import net.spy.memcached.ops.SASLMechsOperation;
import net.spy.memcached.ops.SASLStepOperation;
import net.spy.memcached.ops.SetAttrOperation;
import net.spy.memcached.ops.StatsOperation;
import net.spy.memcached.ops.StoreOperation;
import net.spy.memcached.ops.StoreType;
import net.spy.memcached.ops.TouchOperation;
import net.spy.memcached.ops.VersionOperation;

/**
 * Factory for binary operations.
 */
public class BinaryOperationFactory extends BaseOperationFactory {

  public DeleteOperation delete(String key,
                                OperationCallback operationCallback) {
    return new DeleteOperationImpl(key, operationCallback);
  }

  public FlushOperation flush(int delay, OperationCallback cb) {
    return new FlushOperationImpl(cb);
  }

  public GetOperation get(String key, Callback callback) {
    return new GetOperationImpl(key, callback);
  }

  public GetOperation get(Collection<String> keys, Callback cb, boolean isMGet) {
    return new MultiGetOperationImpl(keys, cb);
  }

  public GetsOperation gets(String key, GetsOperation.Callback cb) {
    return new GetOperationImpl(key, cb);
  }

  public GetsOperation gets(Collection<String> keys, GetsOperation.Callback cb, boolean isMGet) {
    throw new RuntimeException(
            "multiple key gets is not supported in binary protocol yet.");
  }

  public MutatorOperation mutate(Mutator m, String key, int by,
                                 long def, int exp, OperationCallback cb) {
    return new MutatorOperationImpl(m, key, by, def, exp, cb);
  }

  public StatsOperation stats(String arg,
                              net.spy.memcached.ops.StatsOperation.Callback cb) {
    return new StatsOperationImpl(arg, cb);
  }

  public StoreOperation store(StoreType storeType, String key, int flags,
                              int exp, byte[] data, OperationCallback cb) {
    return new StoreOperationImpl(storeType, key, flags, exp, data, 0, cb);
  }

  public TouchOperation touch(String key, int expiration, OperationCallback cb) {
    throw new RuntimeException(
            "TouchOperation is not supported in binary protocol yet.");
  }

  public VersionOperation version(OperationCallback cb) {
    return new VersionOperationImpl(cb);
  }

  public NoopOperation noop(OperationCallback cb) {
    return new NoopOperationImpl(cb);
  }

  public CASOperation cas(StoreType type, String key, long casId, int flags,
                          int exp, byte[] data, OperationCallback cb) {
    return new StoreOperationImpl(type, key, flags, exp, data,
            casId, cb);
  }

  public ConcatenationOperation cat(ConcatenationType catType, long casId,
                                    String key, byte[] data, OperationCallback cb) {
    return new ConcatenationOperationImpl(catType, key, data, casId, cb);
  }

  public SASLAuthOperation saslAuth(SaslClient sc, OperationCallback cb) {
    return new SASLAuthOperationImpl(sc, cb);
  }

  public SASLMechsOperation saslMechs(boolean isInternal, OperationCallback cb) {
    return new SASLMechsOperationImpl(isInternal, cb);
  }

  public SASLStepOperation saslStep(SaslClient sc, byte[] challenge, OperationCallback cb) {
    return new SASLStepOperationImpl(sc, challenge, cb);
  }

  //// UNSUPPORTED ////

  public SetAttrOperation setAttr(String key, Attributes attrs,
                                  OperationCallback cb) {
    throw new RuntimeException(
            "SetAttrOperation is not supported in binary protocol yet.");
  }

  public GetAttrOperation getAttr(String key,
                                  net.spy.memcached.ops.GetAttrOperation.Callback cb) {
    throw new RuntimeException(
            "GetAttrOperation is not supported in binary protocol yet.");
  }

  public CollectionInsertOperation collectionInsert(String key, String subkey,
                                                    CollectionInsert<?> collectionInsert,
                                                    byte[] data,
                                                    OperationCallback cb) {
    throw new RuntimeException(
            "CollectionInsertOperation is not supported in binary protocol yet.");
  }

  public CollectionPipedInsertOperation collectionPipedInsert(String key,
                                                              CollectionPipedInsert<?> insert,
                                                              OperationCallback cb) {
    throw new RuntimeException(
            "CollectionPipedInsertOperation is not supported in binary protocol yet.");
  }

  public CollectionGetOperation collectionGet(String key,
                                              CollectionGet collectionGet,
                                              CollectionGetOperation.Callback cb) {
    throw new RuntimeException(
            "CollectionGetOperation is not supported in binary protocol yet.");
  }

  public CollectionDeleteOperation collectionDelete(String key,
                                                    CollectionDelete collectionDelete,
                                                    OperationCallback cb) {
    throw new RuntimeException(
            "CollectionDeleteOperation is not supported in binary protocol yet.");
  }

  public CollectionExistOperation collectionExist(String key, String subkey,
                                                  CollectionExist collectionExist,
                                                  OperationCallback cb) {
    throw new RuntimeException(
            "CollectionExistOperation is not supported in binary protocol yet.");
  }

  public CollectionCreateOperation collectionCreate(String key,
                                                    CollectionCreate collectionCreate,
                                                    OperationCallback cb) {
    throw new RuntimeException(
            "CollectionCreateOperation is not supported in binary protocol yet.");
  }

  public CollectionCountOperation collectionCount(String key,
                                                  CollectionCount collectionCount,
                                                  OperationCallback cb) {
    throw new RuntimeException(
            "CollectionCountOperation is not supported in binary protocol yet.");
  }

  public FlushOperation flush(String prefix, int delay, boolean noreply, OperationCallback cb) {
    throw new RuntimeException(
            "Flush by prefix operation is not supported in binary protocol yet.");
  }

  @Override
  public BTreeSortMergeGetOperationOld bopsmget(BTreeSMGet<?> smGet,
                                                BTreeSortMergeGetOperationOld.Callback cb) {
    throw new RuntimeException(
            "B+ tree sort merge get operation is not supported in binary protocol yet.");
  }

  @Override
  public BTreeSortMergeGetOperation bopsmget(BTreeSMGet<?> smGet,
                                             BTreeSortMergeGetOperation.Callback cb) {
    throw new RuntimeException(
            "B+ tree sort merge get operation is not supported in binary protocol yet.");
  }

  @Override
  public CollectionUpdateOperation collectionUpdate(String key,
                                                    String subkey,
                                                    CollectionUpdate<?> collectionUpdate,
                                                    byte[] data,
                                                    OperationCallback cb) {
    throw new RuntimeException(
            "Collection update operation is not supported in binary protocol yet.");
  }

  @Override
  public CollectionPipedUpdateOperation collectionPipedUpdate(String key,
                                                              CollectionPipedUpdate<?> update,
                                                              OperationCallback cb) {
    throw new RuntimeException(
            "CollectionPipedUpdateOperation is not supported in binary protocol yet.");
  }

  @Override
  public CollectionMutateOperation collectionMutate(String key,
                                                    String subkey,
                                                    CollectionMutate collectionMutate,
                                                    OperationCallback cb) {
    throw new RuntimeException(
            "Collection mutate(incr/decr) operation is not supported in binary protocol yet.");
  }

  @Override
  public CollectionPipedExistOperation collectionPipedExist(String key,
                                                            SetPipedExist<?> exist,
                                                            OperationCallback cb) {
    throw new RuntimeException(
            "Collection piped exist operation is not supported in binary protocol yet.");
  }

  @Override
  public CollectionBulkInsertOperation collectionBulkInsert(CollectionBulkInsert<?> insert,
                                                            OperationCallback cb) {
    throw new RuntimeException(
            "Collection piped insert2 operation is not supported in binary protocol yet.");
  }

  @Override
  public BTreeGetBulkOperation bopGetBulk(BTreeGetBulk<?> get,
                                          BTreeGetBulkOperation.Callback cb) {
    throw new RuntimeException(
            "BTree get bulk operation is not supported in binary protocol yet.");
  }

  @Override
  public BTreeGetByPositionOperation bopGetByPosition(String key,
                                                      BTreeGetByPosition get,
                                                      OperationCallback cb) {
    throw new RuntimeException(
            "BTree get by position operation is not supported in binary protocol yet.");
  }

  @Override
  public BTreeFindPositionOperation bopFindPosition(String key,
                                                    BTreeFindPosition get, OperationCallback cb) {
    throw new RuntimeException(
            "BTree find position operation is not supported in binary protocol yet.");
  }

  @Override
  public BTreeFindPositionWithGetOperation bopFindPositionWithGet(String key,
                                                                  BTreeFindPositionWithGet get,
                                                                  OperationCallback cb) {
    throw new RuntimeException(
            "BTree find position with get operation is not supported in binary protocol yet.");
  }

  @Override
  public BTreeInsertAndGetOperation bopInsertAndGet(String key,
                                                    BTreeInsertAndGet<?> get, byte[] dataToInsert,
                                                    OperationCallback cb) {
    throw new RuntimeException(
            "BTree insert and get operation is not supported in binary protocol yet.");
  }

}
