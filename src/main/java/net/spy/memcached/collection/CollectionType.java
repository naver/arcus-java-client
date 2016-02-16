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

/**
 * Supported collection types.
 */
public enum CollectionType {

	/**
	 * Key-value
	 */
	kv("kv"),
	/**
	 * List collection
	 */
	list("list"),
	/**
	 * Set collection
	 */
	set("set"),
	/**
	 * B+ tree collection
	 */
	btree("b+tree"),
	/**
	 * Map collection
	 */
	map("map")
	;
	
	String stringValue;
	CollectionType(String stringValue) {
		this.stringValue = stringValue;
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
	
}
