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
import java.util.concurrent.atomic.AtomicReference;

import net.spy.memcached.collection.Element;
import net.spy.memcached.ops.CollectionOperationStatus;

/**
 * Future object that contains an b+tree element object
 * @param <T>
 */
public class BTreeStoreAndGetFuture<T, E> extends CollectionFuture<T> {

	private Element<E> element;

	public BTreeStoreAndGetFuture(CountDownLatch l, long opTimeout) {
		this(l, new AtomicReference<T>(null), opTimeout);
	}
	
	public BTreeStoreAndGetFuture(CountDownLatch l, AtomicReference<T> oref,
			long opTimeout) {
		super(l, oref, opTimeout);
	}
	
	public void set(T o, CollectionOperationStatus status) {
		objRef.set(o);
		opStatus = status;
	}

	public Element<E> getElement() {
		return element;
	}

	public void setElement(Element<E> element) {
		this.element = element;
	}

}
