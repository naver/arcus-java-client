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
import java.util.List;
import java.util.Map;

import javax.security.auth.callback.CallbackHandler;

import net.spy.memcached.collection.Attributes;
import net.spy.memcached.collection.BTreeFindPosition;
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
import net.spy.memcached.ops.BTreeGetBulkOperation;
import net.spy.memcached.ops.BTreeGetByPositionOperation;
import net.spy.memcached.ops.BTreeSortMergeGetOperation;
import net.spy.memcached.ops.BTreeStoreAndGetOperation;
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
import net.spy.memcached.ops.ConcatenationOperation;
import net.spy.memcached.ops.ConcatenationType;
import net.spy.memcached.ops.DeleteOperation;
import net.spy.memcached.ops.ExtendedBTreeGetOperation;
import net.spy.memcached.ops.FlushOperation;
import net.spy.memcached.ops.GetAttrOperation;
import net.spy.memcached.ops.GetOperation;
import net.spy.memcached.ops.GetsOperation;
import net.spy.memcached.ops.KeyedOperation;
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
 * Factory that builds operations for protocol handlers.
 */
public interface OperationFactory {

	/**
	 * Create a NOOP operation.
	 *
	 * @param cb the operation callback
	 * @return the new NoopOperation
	 */
	NoopOperation noop(OperationCallback cb);

	/**
	 * Create a deletion operation.
	 *
	 * @param key the key to delete
	 * @param operationCallback the status callback
	 * @return the new DeleteOperation
	 */
	DeleteOperation delete(String key, OperationCallback operationCallback);

	/**
	 * Create a flush operation.
	 *
	 * @param delay delay until flush.
	 * @param operationCallback the status callback
	 * @return the new FlushOperation
	 */
	FlushOperation flush(int delay, OperationCallback operationCallback);

	/**
	 * Create a get operation.
	 *
	 * @param key the key to get
	 * @param callback the callback that will contain the results
	 * @return a new GetOperation
	 */
	GetOperation get(String key, GetOperation.Callback callback);

	/**
	 * Create a gets operation.
	 *
	 * @param key the key to get
	 * @param callback the callback that will contain the results
	 * @return a new GetsOperation
	 */
	GetsOperation gets(String key, GetsOperation.Callback callback);


	/**
	 * Create a get operation.
	 *
	 * @param keys the collection of keys to get
	 * @param cb the callback that will contain the results
	 * @return a new GetOperation
	 */
	GetOperation get(Collection<String> keys, GetOperation.Callback cb);

	/**
	 * Create a mutator operation.
	 *
	 * @param m the mutator type
	 * @param key the mutatee key
	 * @param by the amount to increment or decrement
	 * @param def the default value
	 * @param exp expiration in case we need to default (0 if no default)
	 * @param cb the status callback
	 * @return the new mutator operation
	 */
	MutatorOperation mutate(Mutator m, String key, int by,
			long def, int exp, OperationCallback cb);

	/**
	 * Get a new StatsOperation.
	 *
	 * @param arg the stat parameter (see protocol docs)
	 * @param cb the stats callback
	 * @return the new StatsOperation
	 */
	StatsOperation stats(String arg, StatsOperation.Callback cb);

	/**
	 * Create a store operation.
	 *
	 * @param storeType the type of store operation
	 * @param key the key to store
	 * @param flags the storage flags
	 * @param exp the expiration time
	 * @param data the data
	 * @param cb the status callback
	 * @return the new store operation
	 */
	StoreOperation store(StoreType storeType, String key, int flags, int exp,
			byte[] data, OperationCallback cb);

	/**
	 * Get a concatenation operation.
	 *
	 * @param catType the type of concatenation to perform.
	 * @param key the key
	 * @param casId the CAS value for an atomic compare-and-cat
	 * @param data the data to store
	 * @param cb a callback for reporting the status
	 * @return thew new ConcatenationOperation
	 */
	ConcatenationOperation cat(ConcatenationType catType, long casId,
			String key, byte[] data, OperationCallback cb);

	/**
	 * Create a CAS operation.
	 *
	 * @param key the key to store
	 * @param casId the CAS identifier value (from a gets operation)
	 * @param flags the storage flags
	 * @param exp the expiration time
	 * @param data the data
	 * @param cb the status callback
	 * @return the new store operation
	 */
	CASOperation cas(StoreType t, String key, long casId, int flags,
			int exp, byte[] data, OperationCallback cb);

	/**
	 * Create a new version operation.
	 */
	VersionOperation version(OperationCallback cb);

	/**
	 * Create a new SASL mechs operation.
	 */
	SASLMechsOperation saslMechs(OperationCallback cb);

	/**
	 * Create a new sasl auth operation.
	 */
	SASLAuthOperation saslAuth(String[] mech, String serverName,
			Map<String, ?> props, CallbackHandler cbh, OperationCallback cb);

	/**
	 * Create a new sasl step operation.
	 */
	SASLStepOperation saslStep(String[] mech, byte[] challenge,
			String serverName, Map<String, ?> props, CallbackHandler cbh,
			OperationCallback cb);

	/**
	 * 
	 * @param key
	 * @param attrs
	 * @param cb
	 * @return
	 */
	SetAttrOperation setAttr(String key, Attributes attrs,
			OperationCallback cb);
	
	/**
	 * 
	 * @param key
	 * @param cb
	 * @return
	 */
	GetAttrOperation getAttr(String key, GetAttrOperation.Callback cb);
	
	/**
	 * 
	 * @param key
	 * @param subkey
	 * @param collectionStore
	 * @param data
	 * @param cb
	 * @return
	 */
	CollectionStoreOperation collectionStore(String key, String subkey, 
			CollectionStore<?> collectionStore, byte[] data, OperationCallback cb);

	/**
	 * 
	 * @param key
	 * @param store
	 * @param cb
	 * @return
	 */
	CollectionPipedStoreOperation collectionPipedStore(String key,
			CollectionPipedStore<?> store, OperationCallback cb);
	
	/**
	 * 
	 * @param key
	 * @param collectionGet
	 * @param cb
	 * @return
	 */
	CollectionGetOperation collectionGet(String key, 
			CollectionGet<?> collectionGet, CollectionGetOperation.Callback cb);

	/**
	 * 
	 * @param key
	 * @param collectionGet
	 * @param cb
	 * @return
	 */
	CollectionGetOperation collectionGet2(String key, 
			CollectionGet<?> collectionGet, ExtendedBTreeGetOperation.Callback cb);
	
	/**
	 * 
	 * @param key
	 * @param collectionDelete
	 * @param cb
	 * @return
	 */
	CollectionDeleteOperation collectionDelete(String key,
			CollectionDelete<?> collectionDelete, OperationCallback cb);

	/**
	 * 
	 * @param key
	 * @param subkey
	 * @param setPipedExist
	 * @param data
	 * @param cb
	 * @return
	 */
	CollectionExistOperation collectionExist(String key, String subkey, 
			CollectionExist<?> collectionExist, OperationCallback cb); 
	
	/**
	 * Clone an operation.
	 *
	 * <p>
	 *   This is used for requeueing operations after a server is found to be
	 *   down.
	 * </p>
	 *
	 * <p>
	 *   Note that it returns more than one operation because a multi-get
	 *   could potentially need to be played against a large number of
	 *   underlying servers.  In this case, there's a separate operation for
	 *   each, and callback fa\u00E7ade to reassemble them.  It is left up to
	 *   the operation pipeline to perform whatever optimization is required
	 *   to turn these back into multi-gets.
	 * </p>
	 *
	 * @param op the operation to clone
	 * @return a new operation for each key in the original operation
	 */
	Collection<Operation> clone(KeyedOperation op);

	/**
	 * 
	 * @param key
	 * @param collectionCreate
	 * @param cb
	 * @return
	 */
	CollectionCreateOperation collectionCreate(String key,
			CollectionCreate collectionCreate, OperationCallback cb);

	/**
	 * 
	 * @param key
	 * @param collectionCount
	 * @param cb
	 * @return
	 */
	CollectionCountOperation collectionCount(String key,
			CollectionCount collectionCount, OperationCallback cb);
	
	/**
	 * 
	 * @param prefix
	 * @param delay delay until flush.
	 * @param noreply
	 * @param cb
	 * @return
	 */
	FlushOperation flush(String prefix, int delay, boolean noreply, OperationCallback cb);
	
	/**
	 * 
	 * @param smget
	 * @param cb
	 * @return
	 */
	BTreeSortMergeGetOperation bopsmget(BTreeSMGet<?> smGet, BTreeSortMergeGetOperation.Callback cb);
	
	/**
	 * 
	 * @param key
	 * @param subkey
	 * @param collectionStore
	 * @param data
	 * @param cb
	 * @return
	 */
	CollectionStoreOperation collectionUpsert(String key, String subkey, 
			CollectionStore<?> collectionStore, byte[] data, OperationCallback cb);

	/**
	 * 
	 * @param key
	 * @param subkey
	 * @param collectionUpdate
	 * @param data
	 * @param cb
	 * @return
	 */
	CollectionUpdateOperation collectionUpdate(String key, String subkey, 
			CollectionUpdate<?> collectionUpdate, byte[] data, OperationCallback cb);

	/**
	 * 
	 * @param key
	 * @param update
	 * @param cb
	 * @return
	 */
	CollectionPipedUpdateOperation collectionPipedUpdate(String key,
			CollectionPipedUpdate<?> update, OperationCallback cb);

	/**
	 *
	 * @param key
	 * @param subkey
	 * @param collectionMutate
	 * @param cb
	 * @return
	 */
	CollectionMutateOperation collectionMutate(String key, String subkey, 
			CollectionMutate collectionMutate, OperationCallback cb);
	
	/**
	 * 
	 * @param key
	 * @param exist
	 * @param cb
	 * @return
	 */
	CollectionPipedExistOperation collectionPipedExist(String key,
			SetPipedExist<?> exist, OperationCallback cb);
	
	/**
	 * 
	 * @param key
	 * @param store
	 * @param cb
	 * @return
	 */
	CollectionBulkStoreOperation collectionBulkStore(List<String> key,
			CollectionBulkStore<?> store, OperationCallback cb);
	
	/**
	 * 
	 * @param get
	 * @param cb
	 * @return
	 */
	BTreeGetBulkOperation bopGetBulk(BTreeGetBulk<?> get, BTreeGetBulkOperation.Callback<?> cb);
	
	/**
	 * 
	 * @param key
	 * @param get
	 * @param cb
	 * @return
	 */
	BTreeGetByPositionOperation bopGetByPosition(String key, BTreeGetByPosition<?> get, OperationCallback cb);

	/**
	 * 
	 * @param key
	 * @param get
	 * @param cb
	 * @return
	 */
	BTreeFindPositionOperation bopFindPosition(String key, BTreeFindPosition get, OperationCallback cb);
	
	/**
	 * 
	 * @param key
	 * @param get
	 * @param cb
	 * @return
	 */
	BTreeStoreAndGetOperation bopStoreAndGet(String key,
			BTreeStoreAndGet<?> get, byte[] dataToStore, OperationCallback cb);
	
}
