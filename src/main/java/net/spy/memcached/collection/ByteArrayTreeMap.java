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

import java.util.Comparator;
import java.util.TreeMap;

public final class ByteArrayTreeMap<K, V> extends TreeMap<K, V> {

	private static final long serialVersionUID = -304580135331634224L;

	public ByteArrayTreeMap(Comparator<? super K> comparator) {
		super(comparator);
	}

	@Override
	public V get(Object key) {
		if (key instanceof byte[]) {
			return super.get(new ByteArrayBKey((byte[]) key));
		} else {
			return super.get(key);
		}
	}
}
