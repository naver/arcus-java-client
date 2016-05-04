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
	 * Set item attributes
	 *
	 * @param key key of an item
	 * @param attrs new attribute information
	 * @param cb the status callback
	 * @return a new SetAttrOperation
	 */
	SetAttrOperation setAttr(String key, Attributes attrs,
			OperationCallback cb);
	
	/**
	 * Get item attributes
	 * 
	 * @param key key of an item
	 * @param cb the callback that will contain the results
	 * @return a new GetAttrOperation
	 */
	GetAttrOperation getAttr(String key, GetAttrOperation.Callback cb);
	
	/**
	 * Store operation for collection items.
	 *
	 * @param key collection item's key
	 * @param subkey element key (list index, b+tree bkey)
	 * @param collectionStore operation parameters (value, eflags, attributes, and so on)
	 * @param data the serialized value
	 * @param cb the status callback
	 * @return a new CollectionStoreOperation
	 */
	CollectionStoreOperation collectionStore(String key, String subkey, 
			CollectionStore<?> collectionStore, byte[] data, OperationCallback cb);

	/**
	 * Pipelined store operation for collection items.
	 *
	 * @param key collection item's key
	 * @param store operation parameters (values, attributes, and so on)
	 * @param cb the status callback
	 * @return a new CollectionPipedStoreOperation
	 */
	CollectionPipedStoreOperation collectionPipedStore(String key,
			CollectionPipedStore<?> store, OperationCallback cb);
	
	/**
	 * Get operation for collection items.
	 *
	 * @param key collection item's key
	 * @param collectionGet operation parameters (element keys and so on)
	 * @param cb the callback that will contain the results
	 * @return a new CollectionGetOperation
	 */
	CollectionGetOperation collectionGet(String key, 
			CollectionGet collectionGet, CollectionGetOperation.Callback cb);

	/**
	 * Delete operation for collection items.
	 * 
	 * @param key collection item's key
	 * @param collectionDelete operation parameters (element index/key, value, and so on)
	 * @param cb the status callback
	 * @return a new CollectionDeleteOperation
	 */
	CollectionDeleteOperation collectionDelete(String key,
			CollectionDelete collectionDelete, OperationCallback cb);

	/**
	 * Existence operation for collection items. 
	 *
	 * @param key collection item's key
	 * @param subkey element key (list index, b+tree bkey)
	 * @param collectionExist operation parameters (element value and so on)
	 * @param cb the callback that will contain the results
	 * @return a new CollectionExistOperation
	 */
	CollectionExistOperation collectionExist(String key, String subkey, 
			CollectionExist collectionExist, OperationCallback cb);
	
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
	 * Create operation for collection items. 
	 *
	 * @param key collection item's key
	 * @param collectionCreate operation parameters (flags, expiration time, and so on)
	 * @param cb the status callback
	 * @return a new CollectionCreateOperation
	 */
	CollectionCreateOperation collectionCreate(String key,
			CollectionCreate collectionCreate, OperationCallback cb);

	/**
	 * Count operation for collection items. 
	 *
	 * @param key collection item's key
	 * @param collectionCount operation parameters (element key range, eflags, and so on)
	 * @param cb the callback that will contain the results
	 * @return a new CollectionCountOperation
	 */
	CollectionCountOperation collectionCount(String key,
			CollectionCount collectionCount, OperationCallback cb);
	
	/**
	 * Flush operation on a given prefix.
	 *
	 * @param prefix prefix of the keys
	 * @param delay the period of time to delay, in seconds
	 * @param noreply flag to make no response
	 * @param cb the status callback
	 * @return a new FlushOperation
	 */
	FlushOperation flush(String prefix, int delay, boolean noreply, OperationCallback cb);
	
	/**
	 * SMGet(Sort-Merge Get) operation for multiple b+tree items. 
	 *
	 * @param smGet smget parameters (keys, eflags, and so on)
	 * @param cb the callback that will contain the results
	 * @return a new BTreeSortMergeGetOperation
	 */
	BTreeSortMergeGetOperationOld bopsmget(BTreeSMGet<?> smGet, BTreeSortMergeGetOperationOld.Callback cb);
	
	/**
	 * SMGet(Sort-Merge Get) operation for multiple b+tree items. 
	 *
	 * @param smGet smget parameters (keys, eflags, and so on)
	 * @param cb the callback that will contain the results
	 * @return a new BTreeSortMergeGetOperation
	 */
	BTreeSortMergeGetOperation bopsmget(BTreeSMGet<?> smGet, BTreeSortMergeGetOperation.Callback cb);
	
	/**
	 * Upsert(Update or Insert) operation for collection items (b+tree items). 
	 *
	 * @param key collection item's key
	 * @param subkey element key (b+tree bkey)
	 * @param collectionStore operation parameters (value, eflag, and so on)
	 * @param data the serialized value
	 * @param cb the status callback
	 * @return a new CollectionStoreOperation
	 */
	CollectionUpsertOperation collectionUpsert(String key, String subkey, 
			CollectionStore<?> collectionStore, byte[] data, OperationCallback cb);

	/**
	 * Update operation for collection items.
	 *
	 * @param key collection item's key
	 * @param subkey element key (list index, b+tree bkey)
	 * @param collectionUpdate (element value and so on)
	 * @param data the serialized value
	 * @param cb the status callback
	 * @return a new CollectionUpdateOperation
	 */
	CollectionUpdateOperation collectionUpdate(String key, String subkey, 
			CollectionUpdate<?> collectionUpdate, byte[] data, OperationCallback cb);

	/**
	 * Pipelined update operation for collection items.
	 *
	 * @param key  collection item's key
	 * @param update operation parameters (values and so on)
	 * @param cb the status callback
	 * @return a new CollectionPipedUpdateOperation
	 */
	CollectionPipedUpdateOperation collectionPipedUpdate(String key,
			CollectionPipedUpdate<?> update, OperationCallback cb);

	/**
	 * Increment/Decrement operation for collection items (b+tree items).
	 *
	 * @param key b+tree item's key
	 * @param subkey element key
	 * @param collectionMutate operation parameters (increment/decrement amount and so on)
	 * @param cb the callback that will contain the incremented/decremented result
	 * @return a new CollectionMutateOperation
	 */
	CollectionMutateOperation collectionMutate(String key, String subkey, 
			CollectionMutate collectionMutate, OperationCallback cb);
	
	/**
	 * Pipelined existence operation for collection items (set items)
	 *
	 * @param key set item's key
	 * @param exist operation parameters (element values)
	 * @param cb the callback that will contain the results
	 * @return a new CollectionPipedExistOperation
	 */
	CollectionPipedExistOperation collectionPipedExist(String key,
			SetPipedExist<?> exist, OperationCallback cb);
	
	/**
	 * Bulk store operation for collection items.
	 *
	 * @param key  collection item's key
	 * @param store operation parameters (element values, and so on)
	 * @param cb the status callback
	 * @return a new CollectionBulkStoreOperation
	 */
	CollectionBulkStoreOperation collectionBulkStore(List<String> key,
			CollectionBulkStore<?> store, OperationCallback cb);
	
	/**
	 * Bulk get opearation for b+tree items.
	 *
	 * @param get operation parameters (item key, element key range, and so on)
	 * @param cb the callback that will contain the results
	 * @return a new BTreeGetBulkOperation
	 */
	BTreeGetBulkOperation bopGetBulk(BTreeGetBulk<?> get, BTreeGetBulkOperation.Callback<?> cb);
	
	/**
	 * Get operation for b+tree items using positions. 
	 *
	 * @param key b+tree item's key
	 * @param get operation parameters (element position and so on)
	 * @param cb the callback that will contain the results
	 * @return a new BTreeGetByPositionOperation
	 */
	BTreeGetByPositionOperation bopGetByPosition(String key, BTreeGetByPosition get, OperationCallback cb);

	/**
	 * Find-position operation for b+tree items. 
	 *
	 * @param key b+tree item's key
	 * @param get operation parameters (element key and so on)
	 * @param cb the callback that will contain the results
	 * @return a new BTreeFindPositionOperation
	 */
	BTreeFindPositionOperation bopFindPosition(String key, BTreeFindPosition get, OperationCallback cb);
	
	/**
	 * Find-position-with-get operation for b+tree items. 
	 *
	 * @param key b+tree item's key
	 * @param get operation parameters (element key and so on)
	 * @param cb the callback that will contain the results
	 * @return a new BTreeFindPositionWithGetOperation
	 */
	BTreeFindPositionWithGetOperation bopFindPositionWithGet(String key, BTreeFindPositionWithGet get, OperationCallback cb);

	/**
	 * Insert/upsert and get the trimmed element for b+tree items.
	 *
	 * @param key b+tree item's key
	 * @param get operation parameters (element key and so on)
	 * @param cb the callback that will contain the results
	 * @return a new BTreeStoreAndGetOperation
	 */
	BTreeStoreAndGetOperation bopStoreAndGet(String key,
			BTreeStoreAndGet<?> get, byte[] dataToStore, OperationCallback cb);
	
}
