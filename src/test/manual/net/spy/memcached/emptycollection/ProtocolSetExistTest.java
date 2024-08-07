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

import junit.framework.TestCase;

import net.spy.memcached.collection.SetExist;
import net.spy.memcached.transcoders.CollectionTranscoder;
import net.spy.memcached.transcoders.Transcoder;

import org.junit.Assert;

public class ProtocolSetExistTest extends TestCase {
  private final Object value = "value";
  private final Transcoder<Object> testTranscoder = new CollectionTranscoder();

  public void testStringify() {
    SetExist<Object> exist = new SetExist<>(value, testTranscoder);
    Assert.assertEquals("5", exist.stringify());
  }

  public void testGetAdditionalArgs() {
    SetExist<Object> exist = new SetExist<>(value, testTranscoder);
    Assert.assertArrayEquals(new byte[]{'v', 'a', 'l', 'u', 'e'}, exist.getAdditionalArgs());
  }
}
