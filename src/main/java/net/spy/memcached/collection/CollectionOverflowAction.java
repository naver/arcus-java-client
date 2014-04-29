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
 * Specifies what to do when the number of elements already reached the Maximum
 * allowable element count in a collection upon insertion.
 */
public enum CollectionOverflowAction {
	/**
	 * If set, the collection storing operation would be failed when the number
	 * of elements already reached the maximum count.
	 */
	error,
	
	/**
	 * <p><b>List Only</b></p>
	 * If set, the new value would be inserted after the deletion of the head
	 * value when the number of elements already reached the maximum count.
	 */
	head_trim,
	
	/**
	 * <p><b>List Only</b></p>
	 * If set, the new value would be inserted after the deletion of the tail
	 * value when the number of elements already reached the maximum count.
	 */
	tail_trim,
	
	/**
	 * <p><b>B+Tree Only</b></p>
	 * If set, the new entry would be inserted after the deletion of the
	 * smallest bkey entry when the number of elements already reached the
	 * maximum count.
	 */
	smallest_trim,
	
	/**
	 * <p><b>B+Tree Only</b></p>
	 * If set, the new entry would be inserted after the deletion of the largest
	 * bkey entry when the number of elements already reached the maximum count.
	 */
	largest_trim,

	/**
	 * <p><b>B+Tree Only</b></p>
	 * If set, the new entry would be inserted after the deletion of the
	 * smallest bkey entry when the number of elements already reached the
	 * maximum count.
	 * but, it does not return a TRIMMED message, even if a btree item was trimmed.
	 */
	smallest_silent_trim,

	/**
	 * <p><b>B+Tree Only</b></p>
	 * If set, the new entry would be inserted after the deletion of the largest
	 * bkey entry when the number of elements already reached the maximum count.
	 * but, it does not return a TRIMMED message, even if a btree item was trimmed.
	 */
	largest_silent_trim,

	/**
	 * 
	 */
	unknown
}
