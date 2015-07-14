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

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Collections;

import net.spy.memcached.KeyUtil;
import net.spy.memcached.collection.CollectionCreate;
import net.spy.memcached.collection.CollectionResponse;
import net.spy.memcached.ops.CollectionCreateOperation;
import net.spy.memcached.ops.CollectionOperationStatus;
import net.spy.memcached.ops.OperationCallback;
import net.spy.memcached.ops.OperationState;
import net.spy.memcached.ops.OperationStatus;
import net.spy.memcached.ops.OperationType;

/**
 * Operation to create empty collection in a memcached server.
 */
public class CollectionCreateOperationImpl extends OperationImpl
	implements CollectionCreateOperation {

	private static final int OVERHEAD = 32;

	private static final OperationStatus STORE_CANCELED = new CollectionOperationStatus(
			false, "collection canceled", CollectionResponse.CANCELED);
	
	private static final OperationStatus CREATED = new CollectionOperationStatus(
			true, "CREATED", CollectionResponse.CREATED);
	private static final OperationStatus EXISTS = new CollectionOperationStatus(
			true, "EXISTS", CollectionResponse.EXISTS);
	private static final OperationStatus SERVER_ERROR = new CollectionOperationStatus(
			false, "SERVER_ERROR", CollectionResponse.SERVER_ERROR);
	
	protected final String key;
	protected final CollectionCreate collectionCreate;
	
	public CollectionCreateOperationImpl(String key, 
			CollectionCreate collectionCreate, OperationCallback cb) {
		super(cb);
		this.key = key;
		this.collectionCreate = collectionCreate;
		setOperationType(OperationType.WRITE);
	}

	@Override
	public void handleLine(String line) {
		assert getState() == OperationState.READING
			: "Read ``" + line + "'' when in " + getState() + " state";
		getCallback().receivedStatus(
				matchStatus(line, CREATED, EXISTS, SERVER_ERROR));
		transitionState(OperationState.COMPLETE);
	}

	@Override
	public void initialize() {
		String args = collectionCreate.stringify();
		ByteBuffer bb = ByteBuffer.allocate(KeyUtil.getKeyBytes(key).length
				+ args.length()
				+ OVERHEAD);
		setArguments(bb, collectionCreate.getCommand(), key, args);
		bb.flip();
		setBuffer(bb);
		
		if (getLogger().isDebugEnabled()) {
			getLogger().debug("Request in ascii protocol: " 
					+ (new String(bb.array())).replaceAll("\\r\\n", ""));
		}
	}

	@Override
	protected void wasCancelled() {
		getCallback().receivedStatus(STORE_CANCELED);
	}
	
	@Override
	public Collection<String> getKeys() {
		return Collections.singleton(key);
	}

	@Override
	public CollectionCreate getCreate() {
		return collectionCreate;
	}
}
