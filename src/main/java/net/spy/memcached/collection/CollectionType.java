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

import java.util.EnumSet;
import java.util.Set;

/**
 * Supported collection types.
 */
public enum CollectionType {

	/**
	 * Key-value
	 */
	kv("kv", null),
	/**
	 * List collection
	 */
	list("list", EnumSet.of(CollectionOverflowAction.error,
							CollectionOverflowAction.head_trim,
							CollectionOverflowAction.tail_trim)),
	/**
	 * Set collection
	 */
	set("set", EnumSet.of(CollectionOverflowAction.error)),
	/**
	 * B+ tree collection
	 */
	btree("b+tree", EnumSet.of(CollectionOverflowAction.error,
								CollectionOverflowAction.smallest_silent_trim,
								CollectionOverflowAction.smallest_trim,
								CollectionOverflowAction.largest_silent_trim,
								CollectionOverflowAction.largest_trim)),
	;

	private Set<CollectionOverflowAction> availableOverflowAction;
	String stringValue;
	CollectionType(String stringValue, Set<CollectionOverflowAction> available) {
		this.stringValue = stringValue;
		this.availableOverflowAction = available;
	}
	
	public String getStringValue() {
		return stringValue;
	}
	
	public static CollectionType find(String value) {
		for (CollectionType each : CollectionType.values()) {
			if (each.stringValue.equals(value)) {
				return each;
			}
		}
		return null;
	}

	public boolean isAvailableOverflowAction(CollectionOverflowAction overflowAction) {
		return availableOverflowAction.contains(overflowAction);
	}
}
