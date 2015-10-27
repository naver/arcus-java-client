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
package net.spy.memcached.internal;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import net.spy.memcached.collection.SMGetTrimKey;
import net.spy.memcached.ops.CollectionOperationStatus;
import net.spy.memcached.ops.Operation;
import net.spy.memcached.ops.OperationState;

public abstract class SMGetFuture<T> implements Future<T> {

	private final Collection<Operation> ops;
	private final long timeout;

	public SMGetFuture(Collection<Operation> ops, long timeout) {
		this.ops = ops;
		this.timeout = timeout;
	}

	@Override
	public T get() throws InterruptedException, ExecutionException {
		try {
			return get(timeout, TimeUnit.MILLISECONDS);
		} catch (TimeoutException e) {
			throw new RuntimeException("Timed out waiting for smget operation", e);
		}
	}

	@Override
	public boolean cancel(boolean ign) {
		boolean rv = false;
		for (Operation op : ops) {
			op.cancel();
			rv |= op.getState() == OperationState.WRITING;
		}
		return rv;
	}

	@Override
	public boolean isCancelled() {
		boolean rv = false;
		for (Operation op : ops) {
			rv |= op.isCancelled();
		}
		return rv;
	}

	@Override
	public boolean isDone() {
		boolean rv = true;
		for (Operation op : ops) {
			rv &= op.getState() == OperationState.COMPLETE;
		}
		return rv || isCancelled();
	}

	public abstract Map<String, CollectionOperationStatus> getMissedKeys();

	public abstract List<String> getMissedKeyList();

	public abstract List<SMGetTrimKey> getTrimmedKeys();
	
	public abstract CollectionOperationStatus getOperationStatus();
}