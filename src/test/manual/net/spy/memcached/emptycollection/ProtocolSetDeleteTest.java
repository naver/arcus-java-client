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
package net.spy.memcached.emptycollection;

import org.junit.Assert;
import junit.framework.TestCase;
import net.spy.memcached.collection.SetDelete;
import net.spy.memcached.transcoders.CollectionTranscoder;
import net.spy.memcached.transcoders.Transcoder;

public class ProtocolSetDeleteTest extends TestCase {
	Object value = "value";
	Transcoder<Object> testTranscoder = new CollectionTranscoder();

	public void testStringify() {
		// default setting : dropIfEmpty = true

		SetDelete<Object> del = new SetDelete<Object>(value, false, testTranscoder);
		Assert.assertEquals("5 drop", del.stringify());

		del = new SetDelete<Object>(value, false, false, testTranscoder);
		Assert.assertEquals("5", del.stringify());

		del = new SetDelete<Object>(value, false, true, testTranscoder);
		Assert.assertEquals("5 drop", del.stringify());

		del = new SetDelete<Object>(value, true, testTranscoder);
		Assert.assertEquals("5 drop noreply", del.stringify());

		del = new SetDelete<Object>(value, true, false, testTranscoder);
		Assert.assertEquals("5 noreply", del.stringify());

		del = new SetDelete<Object>(value, true, true, testTranscoder);
		Assert.assertEquals("5 drop noreply", del.stringify());
	}

	public void testGetAdditionalArgs() {
		byte[] expected = new byte[]{'v','a','l','u','e'};
		SetDelete<Object> del = new SetDelete<Object>(value, false, testTranscoder);
		Assert.assertArrayEquals(expected, del.getAdditionalArgs());

		del = new SetDelete<Object>(value, false, false, testTranscoder);
		Assert.assertArrayEquals(expected, del.getAdditionalArgs());

		del = new SetDelete<Object>(value, false, true, testTranscoder);
		Assert.assertArrayEquals(expected, del.getAdditionalArgs());

		del = new SetDelete<Object>(value, true, testTranscoder);
		Assert.assertArrayEquals(expected, del.getAdditionalArgs());

		del = new SetDelete<Object>(value, true, false, testTranscoder);
		Assert.assertArrayEquals(expected, del.getAdditionalArgs());

		del = new SetDelete<Object>(value, true, true, testTranscoder);
		Assert.assertArrayEquals(expected, del.getAdditionalArgs());
	}
}
