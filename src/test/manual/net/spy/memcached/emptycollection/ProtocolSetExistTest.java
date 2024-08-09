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

import net.spy.memcached.collection.SetExist;
import net.spy.memcached.transcoders.CollectionTranscoder;
import net.spy.memcached.transcoders.Transcoder;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class ProtocolSetExistTest {
  private final Object value = "value";
  private final Transcoder<Object> testTranscoder = new CollectionTranscoder();

  @Test
  public void testStringify() {
    SetExist<Object> exist = new SetExist<>(value, testTranscoder);
    Assertions.assertEquals("5", exist.stringify());
  }

  @Test
  public void testGetAdditionalArgs() {
    SetExist<Object> exist = new SetExist<>(value, testTranscoder);
    Assertions.assertArrayEquals(new byte[]{'v', 'a', 'l', 'u', 'e'}, exist.getAdditionalArgs());
  }
}
