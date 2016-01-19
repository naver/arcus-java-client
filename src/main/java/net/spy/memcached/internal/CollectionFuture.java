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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

import net.spy.memcached.MemcachedConnection;
import net.spy.memcached.ops.CollectionOperationStatus;
import net.spy.memcached.ops.Operation;
import net.spy.memcached.ops.OperationState;

/**
 * Managed future for collection operations.
 * 
 * Not intended for general use.
 * 
 * @param <T> Type of object returned from this future.
 */
public class CollectionFuture<T> implements Future<T> {

	protected final CountDownLatch latch;
	protected final AtomicReference<T> objRef;
	protected final long timeout;
	protected Operation op;
	protected CollectionOperationStatus opStatus;

	public CollectionFuture(CountDownLatch l, long opTimeout) {
		this(l, new AtomicReference<T>(null), opTimeout);
	}

	public CollectionFuture(CountDownLatch l, AtomicReference<T> oref,
		long opTimeout) {
		super();
		latch=l;
		objRef=oref;
		timeout = opTimeout;
	}

	public boolean cancel(boolean ign) {
		assert op != null : "No operation";
		op.cancel("by application.");
		// This isn't exactly correct, but it's close enough.  If we're in
		// a writing state, we *probably* haven't started.
		return op.getState() == OperationState.WRITING;
	}

	public T get() throws InterruptedException, ExecutionException {
		try {
			return get(timeout, TimeUnit.MILLISECONDS);
		} catch (TimeoutException e) {
			throw new RuntimeException(
				"Timed out waiting for operation. >" + timeout, e);
		}
	}

	public T get(long duration, TimeUnit units)
		throws InterruptedException, TimeoutException, ExecutionException {
		if(!latch.await(duration, units)) {
			// whenever timeout occurs, continuous timeout counter will increase by 1.
			MemcachedConnection.opTimedOut(op);
			throw new CheckedOperationTimeoutException(
					"Timed out waiting for operation. >" + duration, op);
		} else {
			// continuous timeout counter will be reset
		    MemcachedConnection.opSucceeded(op);
		}
		if(op != null && op.hasErrored()) {
			throw new ExecutionException(op.getException());
		}
		if(op != null && op.isCancelled()) {
			throw new ExecutionException(new RuntimeException(op.getCancelCause()));
		}

		return objRef.get();
	}

	public void set(T o, CollectionOperationStatus status) {
		objRef.set(o);
		opStatus = status;
	}

	public void setOperation(Operation to) {
		op=to;
	}
	
	public boolean isCancelled() {
		assert op != null : "No operation";
		return op.isCancelled();
	}

	public boolean isDone() {
		assert op != null : "No operation";
		return latch.getCount() == 0 ||
			op.isCancelled() || op.getState() == OperationState.COMPLETE;
	}

	public CollectionOperationStatus getOperationStatus() {
		return (op.getState() == OperationState.COMPLETE)? opStatus : null;
	}
	
}
