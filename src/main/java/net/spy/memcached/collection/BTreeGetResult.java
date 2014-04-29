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
package net.spy.memcached.collection;

import java.util.Map;
import java.util.SortedMap;

import net.spy.memcached.ops.CollectionOperationStatus;

public class BTreeGetResult<K, V> {

	private final SortedMap<K, BTreeElement<K, V>> elements;
	private final CollectionOperationStatus opStatus;

	public BTreeGetResult(SortedMap<K, BTreeElement<K, V>> elements,
			CollectionOperationStatus opStatus) {
		this.elements = elements;
		this.opStatus = opStatus;
	}

	public Map<K, BTreeElement<K, V>> getElements() {
		return elements;
	}

	public CollectionOperationStatus getCollectionResponse() {
		return opStatus;
	}

	public void addElement(BTreeElement<K, V> element) {
		this.elements.put(element.getBkey(), element);
	}
}
