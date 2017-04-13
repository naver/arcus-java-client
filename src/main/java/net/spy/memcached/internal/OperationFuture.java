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
import net.spy.memcached.ops.Operation;
import net.spy.memcached.ops.OperationState;

/**
 * Managed future for operations.
 *
 * Not intended for general use.
 *
 * @param <T> Type of object returned from this future.
 */
public class OperationFuture<T> implements Future<T> {

	private final CountDownLatch latch;
	private final AtomicReference<T> objRef;
	private final long timeout;
	private Operation op;

	public OperationFuture(CountDownLatch l, long opTimeout) {
		this(l, new AtomicReference<T>(null), opTimeout);
	}

	public OperationFuture(CountDownLatch l, AtomicReference<T> oref,
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
				"Timed out waiting for operation. >" + timeout + " " + TimeUnit.MILLISECONDS, e);
		}
	}

	public T get(long duration, TimeUnit units)
		throws InterruptedException, TimeoutException, ExecutionException {
		if(!latch.await(duration, units)) {
			// whenever timeout occurs, continuous timeout counter will increase by 1.
			MemcachedConnection.opTimedOut(op);
			throw new CheckedOperationTimeoutException(
					"Timed out waiting for operation. >" + duration + " " + units, op);
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

	public void set(T o) {
		objRef.set(o);
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
}
