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

import java.nio.ByteBuffer;

import net.spy.memcached.KeyUtil;
import net.spy.memcached.compat.SpyObject;

public class CollectionObject extends SpyObject {

	protected static final byte[] CRLF={'\r', '\n'};
	
	/**
	 * Set some arguments for an operation into the given byte buffer.
	 */
	protected final void setArguments(ByteBuffer bb, Object... args) {
		boolean wasFirst=true;
		for(Object o : args) {
			String s = String.valueOf(o);
			if(wasFirst) {
				wasFirst=false;
			} else if (!"".equals(s)) {
				bb.put((byte)' ');
			}
			bb.put(KeyUtil.getKeyBytes(s));
		}
		bb.put(CRLF);
	}
	
}
