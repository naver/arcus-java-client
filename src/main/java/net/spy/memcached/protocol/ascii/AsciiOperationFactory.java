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
package net.spy.memcached.protocol.ascii;

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
import net.spy.memcached.ops.GetsOperation;
import net.spy.memcached.ops.KeyedOperation;
import net.spy.memcached.ops.MultiGetOperationCallback;
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
 * Operation factory for the ascii protocol.
 */
public class AsciiOperationFactory extends BaseOperationFactory {

	public DeleteOperation delete(String key, OperationCallback cb) {
		return new DeleteOperationImpl(key, cb);
	}

	public FlushOperation flush(int delay, OperationCallback cb) {
		return new FlushOperationImpl(delay, cb);
	}

	public GetOperation get(String key, GetOperation.Callback cb) {
		return new GetOperationImpl(key, cb);
	}

	public GetOperation get(Collection<String> keys, GetOperation.Callback cb) {
		return new GetOperationImpl(keys, cb);
	}

	public GetsOperation gets(String key, GetsOperation.Callback cb) {
		 return new GetsOperationImpl(key, cb);
	}

	public MutatorOperation mutate(Mutator m, String key, int by,
			long def, int exp, OperationCallback cb) {
		return new MutatorOperationImpl(m, key, by, def, exp, cb);
	}

	public StatsOperation stats(String arg, StatsOperation.Callback cb) {
		return new StatsOperationImpl(arg, cb);
	}

	public StoreOperation store(StoreType storeType, String key, int flags,
			int exp, byte[] data, OperationCallback cb) {
		return new StoreOperationImpl(storeType, key, flags, exp, data, cb);
	}

	public VersionOperation version(OperationCallback cb) {
		return new VersionOperationImpl(cb);
	}

	public NoopOperation noop(OperationCallback cb) {
		return new VersionOperationImpl(cb);
	}

	public CASOperation cas(StoreType type, String key, long casId, int flags,
			int exp, byte[] data, OperationCallback cb) {
		return new CASOperationImpl(key, casId, flags, exp, data, cb);
	}

	public ConcatenationOperation cat(ConcatenationType catType,
			long casId,
			String key, byte[] data, OperationCallback cb) {
		return new ConcatenationOperationImpl(catType, key, data, cb);
	}

	@Override
	protected Collection<? extends Operation> cloneGet(KeyedOperation op) {
		Collection<Operation> rv=new ArrayList<Operation>();
		GetOperation.Callback callback = new MultiGetOperationCallback(
				op.getCallback(), op.getKeys().size());
		for(String k : op.getKeys()) {
			rv.add(get(k, callback));
		}
		return rv;
	}

	public SASLMechsOperation saslMechs(OperationCallback cb) {
		throw new UnsupportedOperationException();
	}

	public SASLStepOperation saslStep(String[] mech, byte[] challenge,
			String serverName, Map<String, ?> props, CallbackHandler cbh,
			OperationCallback cb) {
		throw new UnsupportedOperationException();
	}

	public SASLAuthOperation saslAuth(String[] mech, String serverName,
			Map<String, ?> props, CallbackHandler cbh, OperationCallback cb) {
		throw new UnsupportedOperationException();
	}

	public SetAttrOperation setAttr(String key, Attributes attrs,
			OperationCallback cb) {
		return new SetAttrOperationImpl(key, attrs, cb);
	}

	public GetAttrOperation getAttr(String key, GetAttrOperation.Callback cb) {
		return new GetAttrOperationImpl(key, cb);
	}
	
	public CollectionStoreOperation collectionStore(String key, String subkey,
			CollectionStore<?> collectionStore, byte[] data, OperationCallback cb) {
		return new CollectionStoreOperationImpl(key, subkey,
				collectionStore, data, cb);
	}
	
	public CollectionPipedStoreOperation collectionPipedStore(String key,
			CollectionPipedStore<?> store, OperationCallback cb) {
		return new CollectionPipedStoreOperationImpl(key, store, cb);
	}

	public CollectionGetOperation collectionGet(String key,
			CollectionGet collectionGet, CollectionGetOperation.Callback cb) {
		return new CollectionGetOperationImpl(key, collectionGet, cb);
	}

	public CollectionDeleteOperation collectionDelete(String key,
			CollectionDelete collectionDelete, OperationCallback cb) {
		return new CollectionDeleteOperationImpl(key, collectionDelete, cb);
	}
	
	public CollectionExistOperation collectionExist(String key, String subkey,
			CollectionExist collectionExist, OperationCallback cb) {
		return new CollectionExistOperationImpl(key, subkey, collectionExist, cb);
	}

	public CollectionCreateOperation collectionCreate(String key,
			CollectionCreate collectionCreate, OperationCallback cb) {
		return new CollectionCreateOperationImpl(key, collectionCreate, cb);
	}
	
	public CollectionCountOperation collectionCount(String key,
			CollectionCount collectionCount, OperationCallback cb) {
		return new CollectionCountOperationImpl(key, collectionCount, cb);
	}
	
	public FlushOperation flush(String prefix, int delay, boolean noreply, OperationCallback cb) {
		return new FlushByPrefixOperationImpl(prefix, delay, noreply, cb);
	}

	public BTreeSortMergeGetOperationOld bopsmget(BTreeSMGet<?> smGet,
			BTreeSortMergeGetOperationOld.Callback cb) {
		return new BTreeSortMergeGetOperationOldImpl(smGet, cb);
	}
	
	public BTreeSortMergeGetOperation bopsmget(BTreeSMGet<?> smGet,
			BTreeSortMergeGetOperation.Callback cb) {
		return new BTreeSortMergeGetOperationImpl(smGet, cb);
	}

	@Override
	public CollectionUpsertOperation collectionUpsert(String key, String subkey,
			CollectionStore<?> collectionStore, byte[] data,
			OperationCallback cb) {
		return new CollectionUpsertOperationImpl(key, subkey, collectionStore,
				data, cb);
	}

	@Override
	public CollectionUpdateOperation collectionUpdate(String key,
			String subkey, CollectionUpdate<?> collectionUpdate, byte[] data,
			OperationCallback cb) {
		return new CollectionUpdateOperationImpl(key, subkey, collectionUpdate,
				data, cb);
	}

	@Override
	public CollectionPipedUpdateOperation collectionPipedUpdate(String key,
			CollectionPipedUpdate<?> update, OperationCallback cb) {
		return new CollectionPipedUpdateOperationImpl(key, update, cb);
	}
	
	@Override
	public CollectionPipedExistOperation collectionPipedExist(String key,
			SetPipedExist<?> exist, OperationCallback cb) {
		return new CollectionPipedExistOperationImpl(key, exist, cb);
	}

	@Override
	public CollectionBulkStoreOperation collectionBulkStore(List<String> key,
			CollectionBulkStore<?> store, OperationCallback cb) {
		return new CollectionBulkStoreOperationImpl(key, store, cb);
	}

	@Override
	public BTreeGetBulkOperation bopGetBulk(BTreeGetBulk<?> getBulk,
			BTreeGetBulkOperation.Callback<?> cb) {
		return new BTreeGetBulkOperationImpl(getBulk, cb);
	}

	@Override
	public CollectionMutateOperation collectionMutate(String key,
			String subkey, CollectionMutate collectionMutate,
			OperationCallback cb) {
		return new CollectionMutateOperationImpl(key, subkey, collectionMutate, cb);
	}

	@Override
	public BTreeGetByPositionOperation bopGetByPosition(String key,
			BTreeGetByPosition get, OperationCallback cb) {
		return new BTreeGetByPositionOperationImpl(key, get, cb);
	}

	@Override
	public BTreeFindPositionOperation bopFindPosition(String key,
			BTreeFindPosition get, OperationCallback cb) {
		return new BTreeFindPositionOperationImpl(key, get, cb);
	}

	@Override
	public BTreeFindPositionWithGetOperation bopFindPositionWithGet(String key,
			BTreeFindPositionWithGet get, OperationCallback cb) {
		return new BTreeFindPositionWithGetOperationImpl(key, get, cb);
	}

	@Override
	public BTreeStoreAndGetOperation bopStoreAndGet(String key,
			BTreeStoreAndGet<?> get, byte[] dataToStore, OperationCallback cb) {
		return new BTreeStoreAndGetOperationImpl(key, get, dataToStore, cb);
	}
	
}
