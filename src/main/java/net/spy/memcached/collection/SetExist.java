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

import net.spy.memcached.transcoders.Transcoder;

public class SetExist<T> extends CollectionExist {

	private static final String command = "sop exist";

	protected T value;
	protected byte[] additionalArgs;
	protected Transcoder<T> tc;
	
	public SetExist(T value, Transcoder<T> tc) {
		this.value = value;
		this.tc = tc;
		this.additionalArgs = tc.encode(value).getData();
	}

	@Override
	public byte[] getAdditionalArgs() {
		return additionalArgs;
	}

	public String stringify() {
		return String.valueOf(additionalArgs.length);
	}

	public String getCommand() {
		return command;
	}
	
}
