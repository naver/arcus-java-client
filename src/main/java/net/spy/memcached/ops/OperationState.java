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
package net.spy.memcached.ops;

/**
 * State of this operation.
 */
public enum OperationState {
	/**
	 * State indicating this operation is writing data to the server.
	 */
	WRITING,
	/**
	 * State indicating this operation is reading data from the server.
	 */
	READING,
	/**
	 * State indicating this operation is complete.
	 */
	COMPLETE,
	/**
	 * State indicating this operation timed out without completing.
	 */
	TIMEDOUT
	/* ENABLE_REPLICATION if */
	/* WHCHOI83_MEMCACHED_REPLICA_GROUP if */
	/**
	 * State indicating this operation will be moved by switchover or failover
	 */
	, MOVING
	/* WHCHOI83_MEMCACHED_REPLICA_GROUP end */
	/* ENABLE_REPLICATION end */
}