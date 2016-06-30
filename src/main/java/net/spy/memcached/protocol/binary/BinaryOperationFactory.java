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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import javax.security.auth.callback.CallbackHandler;

import net.spy.memcached.collection.Attributes;
import net.spy.memcached.collection.BTreeFindPosition;
import net.spy.memcached.collection.BTreeFindPositionWithGet;
import net.spy.memcached.collection.BTreeGetBulk;
import net.spy.memcached.collection.BTreeGetByPosition;
import net.spy.memcached.collection.BTreeSMGet;
import net.spy.memcached.collection.BTreeStoreAndGet;
import net.spy.memcached.collection.CollectionBulkStore;
import net.spy.memcached.collection.CollectionCount;
import net.spy.memcached.collection.CollectionCreate;
import net.spy.memcached.collection.CollectionDelete;
import net.spy.memcached.collection.CollectionExist;
import net.spy.memcached.collection.CollectionGet;
import net.spy.memcached.collection.CollectionMutate;
import net.spy.memcached.collection.CollectionPipedStore;
import net.spy.memcached.collection.CollectionPipedUpdate;
import net.spy.memcached.collection.CollectionStore;
import net.spy.memcached.collection.CollectionUpdate;
import net.spy.memcached.collection.SetPipedExist;
import net.spy.memcached.ops.BTreeFindPositionOperation;
import net.spy.memcached.ops.BTreeFindPositionWithGetOperation;
import net.spy.memcached.ops.BTreeGetBulkOperation;
import net.spy.memcached.ops.BTreeGetByPositionOperation;
import net.spy.memcached.ops.BTreeSortMergeGetOperation;
import net.spy.memcached.ops.BTreeSortMergeGetOperationOld;
import net.spy.memcached.ops.BTreeStoreAndGetOperation;
import net.spy.memcached.ops.BaseOperationFactory;
import net.spy.memcached.ops.CASOperation;
import net.spy.memcached.ops.CollectionBulkStoreOperation;
import net.spy.memcached.ops.CollectionCountOperation;
import net.spy.memcached.ops.CollectionCreateOperation;
import net.spy.memcached.ops.CollectionDeleteOperation;
import net.spy.memcached.ops.CollectionExistOperation;
import net.spy.memcached.ops.CollectionGetOperation;
import net.spy.memcached.ops.CollectionMutateOperation;
import net.spy.memcached.ops.CollectionPipedExistOperation;
import net.spy.memcached.ops.CollectionPipedStoreOperation;
import net.spy.memcached.ops.CollectionPipedUpdateOperation;
import net.spy.memcached.ops.CollectionStoreOperation;
import net.spy.memcached.ops.CollectionUpdateOperation;
import net.spy.memcached.ops.CollectionUpsertOperation;
import net.spy.memcached.ops.ConcatenationOperation;
import net.spy.memcached.ops.ConcatenationType;
import net.spy.memcached.ops.DeleteOperation;
import net.spy.memcached.ops.FlushOperation;
import net.spy.memcached.ops.GetAttrOperation;
import net.spy.memcached.ops.GetOperation;
import net.spy.memcached.ops.GetOperation.Callback;
import net.spy.memcached.ops.GetsOperation;
import net.spy.memcached.ops.KeyedOperation;
import net.spy.memcached.ops.MultiGetOperationCallback;
import net.spy.memcached.ops.MultiGetsOperationCallback;
import net.spy.memcached.ops.Mutator;
import net.spy.memcached.ops.MutatorOperation;
import net.spy.memcached.ops.NoopOperation;
import net.spy.memcached.ops.Operation;
import net.spy.memcached.ops.OperationCallback;
import net.spy.memcached.ops.SASLAuthOperation;
import net.spy.memcached.ops.SASLMechsOperation;
import net.spy.memcached.ops.SASLStepOperation;
import net.spy.memcached.ops.SetAttrOperation;
import net.spy.memcached.ops.StatsOperation;
import net.spy.memcached.ops.StoreOperation;
import net.spy.memcached.ops.StoreType;
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

	public GetOperation get(Collection<String> value, Callback cb) {
		return new MultiGetOperationImpl(value, cb);
	}

	public GetsOperation gets(String key, GetsOperation.Callback cb) {
		return new GetOperationImpl(key, cb);
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

	@Override
	protected Collection<? extends Operation> cloneGet(KeyedOperation op) {
		Collection<Operation> rv=new ArrayList<Operation>();
		GetOperation.Callback getCb = null;
		GetsOperation.Callback getsCb = null;
		if(op.getCallback() instanceof GetOperation.Callback) {
			getCb=new MultiGetOperationCallback(
					op.getCallback(), op.getKeys().size());
		} else {
			getsCb=new MultiGetsOperationCallback(
					op.getCallback(), op.getKeys().size());
		}
		for(String k : op.getKeys()) {
			rv.add(getCb == null ? gets(k, getsCb) : get(k, getCb));
		}
		return rv;
	}

	public SASLAuthOperation saslAuth(String[] mech, String serverName,
			Map<String, ?> props, CallbackHandler cbh, OperationCallback cb) {
		return new SASLAuthOperationImpl(mech, serverName, props, cbh, cb);
	}

	public SASLMechsOperation saslMechs(OperationCallback cb) {
		return new SASLMechsOperationImpl(cb);
	}

	public SASLStepOperation saslStep(String[] mech, byte[] challenge,
			String serverName, Map<String, ?> props, CallbackHandler cbh,
			OperationCallback cb) {
		return new SASLStepOperationImpl(mech, challenge, serverName,
				props, cbh, cb);
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

	public CollectionStoreOperation collectionStore(String key, String subkey,
			CollectionStore<?> collectionStore, byte[] data,
			OperationCallback cb) {
		throw new RuntimeException(
			"CollectionStoreOperation is not supported in binary protocol yet.");
	}
	
	public CollectionStoreOperation collectionStore(String key, byte[] subkey,
			CollectionStore<?> collectionStore, byte[] data,
			OperationCallback cb) {
		throw new RuntimeException(
				"CollectionStoreOperation is not supported in binary protocol yet.");
	}

	public CollectionPipedStoreOperation collectionPipedStore(String key,
			CollectionPipedStore<?> store, OperationCallback cb) {
		throw new RuntimeException(
			"CollectionPipedStoreOperation is not supported in binary protocol yet.");
	}
	
	public CollectionGetOperation collectionGet(String key,
			CollectionGet collectionGet,
			net.spy.memcached.ops.CollectionGetOperation.Callback cb) {
		throw new RuntimeException(
			"CollectionGetOperation is not supported in binary protocol yet.");
	}

	public CollectionDeleteOperation collectionDelete(String key,
			CollectionDelete collectionDelete, OperationCallback cb) {
		throw new RuntimeException(
			"CollectionDeleteOperation is not supported in binary protocol yet.");
	}

	public CollectionExistOperation collectionExist(String key, String subkey,
			CollectionExist collectionExist, OperationCallback cb) {
		throw new RuntimeException(
			"CollectionExistOperation is not supported in binary protocol yet.");
	}

	public CollectionCreateOperation collectionCreate(String key,
			CollectionCreate collectionCreate, OperationCallback cb) {
		throw new RuntimeException(
		"CollectionCreateOperation is not supported in binary protocol yet.");
	}

	public CollectionCountOperation collectionCount(String key,
			CollectionCount collectionCount, OperationCallback cb) {
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
	public CollectionUpsertOperation collectionUpsert(String key, String subkey,
			CollectionStore<?> collectionStore, byte[] data,
			OperationCallback cb) {
		throw new RuntimeException(
				"B+ tree upsert operation is not supported in binary protocol yet.");
	}

	@Override
	public CollectionUpdateOperation collectionUpdate(String key,
			String subkey, CollectionUpdate<?> collectionUpdate, byte[] data,
			OperationCallback cb) {
		throw new RuntimeException(
				"Collection update operation is not supported in binary protocol yet.");
	}

	@Override
	public CollectionPipedUpdateOperation collectionPipedUpdate(String key,
			CollectionPipedUpdate<?> update, OperationCallback cb) {
		throw new RuntimeException(
				"CollectionPipedStoreOperation is not supported in binary protocol yet.");
	}

	@Override
	public CollectionMutateOperation collectionMutate(String key,
			String subkey, CollectionMutate collectionMutate, OperationCallback cb) {
		throw new RuntimeException(
				"Collection mutate(incr/decr) operation is not supported in binary protocol yet.");
	}
	
	@Override
	public CollectionPipedExistOperation collectionPipedExist(String key,
			SetPipedExist<?> exist, OperationCallback cb) {
		throw new RuntimeException(
				"Collection piped exist operation is not supported in binary protocol yet.");
	}

	@Override
	public CollectionBulkStoreOperation collectionBulkStore(
			List<String> key, CollectionBulkStore<?> store,
			OperationCallback cb) {
		throw new RuntimeException(
				"Collection piped store2 operation is not supported in binary protocol yet.");
	}

	@Override
	public BTreeGetBulkOperation bopGetBulk(BTreeGetBulk<?> get,
			BTreeGetBulkOperation.Callback<?> cb) {
		throw new RuntimeException(
				"BTree get bulk operation is not supported in binary protocol yet.");
	}

	@Override
	public BTreeGetByPositionOperation bopGetByPosition(String key,
			BTreeGetByPosition get, OperationCallback cb) {
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
			BTreeFindPositionWithGet get, OperationCallback cb) {
		throw new RuntimeException(
				"BTree find position with get operation is not supported in binary protocol yet.");
	}

	@Override
	public BTreeStoreAndGetOperation bopStoreAndGet(String key,
			BTreeStoreAndGet<?> get, byte[] dataToStore, OperationCallback cb) {
		throw new RuntimeException(
				"BTree store and get operation is not supported in binary protocol yet.");
	}
	
}
