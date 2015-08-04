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

import java.util.HashSet;
import java.util.Set;

/**
 * Operation status types for collections
 */
public enum CollectionResponse {

	OK,
	END,
	NOT_FOUND,
	NOT_FOUND_ELEMENT,
	ELEMENT_EXISTS,
	CREATED_STORED,
	STORED,
	REPLACED,
	DELETED,
	DELETED_DROPPED,
	TYPE_MISMATCH,
	OVERFLOWED,
	OUT_OF_RANGE,
	ATTR_ERROR_NOT_FOUND,
	ATTR_ERROR_BAD_VALUE,
	EXIST,
	NOT_EXIST,
	
	UNDEFINED,
	CANCELED,
	
	INTERRUPT_EXCEPTION,
	EXECUTION_EXCEPTION,
	TIMEOUT_EXCEPTION,
	EXCEPTION,
	
	UPDATED,
	BKEY_MISMATCH,
	EFLAG_MISMATCH,
	
	CREATED,
	EXISTS,
	SERVER_ERROR,
	
	/**
	 * Command pipelining result
	 */
	RESPONSE,
	
	/**
	 * read only collection
	 */
	UNREADABLE,
	
	DUPLICATED,
	TRIMMED,
	DUPLICATED_TRIMMED,
	ATTR_MISMATCH,
	NOTHING_TO_UPDATE;
	
	private static final Set<String> ENUM_STRINGS;

	static {
		CollectionResponse[] values = CollectionResponse.values();
		ENUM_STRINGS = new HashSet<String>(values.length);
		for (CollectionResponse e : values) {
			ENUM_STRINGS.add(e.toString());
		}
	}

	public static CollectionResponse resolve(String s) {
		if (ENUM_STRINGS.contains(s)) {
			return CollectionResponse.valueOf(s);
		} else {
			return UNDEFINED;
		}
	}
}
