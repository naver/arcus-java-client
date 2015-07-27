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
/* ENABLE_REPLICATION start */
package net.spy.memcached;

/**
 * Read priority for read operation
 */
public enum ReadPriority {

	/**
	 * Read on Master
	 */
	MASTER,

	/**
	 * Read on Slave
	 */
	SLAVE,

	/**
	 * Read RR(Round-robin) pattern.
	 */
	RR, 

	// for API
	NONE
}
/* ENABLE_REPLICATION end */
