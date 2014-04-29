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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import net.spy.memcached.CachedData;
import net.spy.memcached.KeyUtil;
import net.spy.memcached.transcoders.Transcoder;

public class SetPipedExist<T> extends CollectionObject {

	public static final int MAX_PIPED_ITEM_COUNT = 500;

	private static final String COMMAND = "sop exist";
	private static final String PIPE = "pipe";

	private final String key;
	private final List<T> values;
	private final Transcoder<T> tc;
	private int itemCount;

	public List<T> getValues() {
		return this.values;
	}

	public int getItemCount() {
		return this.itemCount;
	}

	public SetPipedExist(String key, List<T> values, Transcoder<T> tc) {
		this.key = key;
		this.values = values;
		this.tc = tc;
		this.itemCount = values.size();
	}

	public ByteBuffer getAsciiCommand() {
		int capacity = 0;

		// decode values
		Collection<byte[]> encodedList = new ArrayList<byte[]>(values.size());
		CachedData cd = null;
		for (T each : values) {
			cd = tc.encode(each);
			encodedList.add(cd.getData());
		}

		// estimate the buffer capacity
		for (byte[] each : encodedList) {
			capacity += KeyUtil.getKeyBytes(key).length;
			capacity += each.length;
			capacity += 64;
		}

		// allocate the buffer
		ByteBuffer bb = ByteBuffer.allocate(capacity);

		// create ascii operation string
		Iterator<byte[]> iterator = encodedList.iterator();
		while (iterator.hasNext()) {
			byte[] each = iterator.next();

			setArguments(bb, COMMAND, key, each.length,
					(iterator.hasNext()) ? PIPE : "");
			bb.put(each);
			bb.put(CRLF);
		}
		// flip the buffer
		bb.flip();

		return bb;
	}

	public ByteBuffer getBinaryCommand() {
		throw new RuntimeException("not supported in binary protocol yet.");
	}
}
