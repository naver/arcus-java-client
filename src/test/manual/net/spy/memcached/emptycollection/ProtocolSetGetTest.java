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

import net.spy.memcached.collection.SetGet;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class ProtocolSetGetTest {

  private static final int count = 10;

  @Test
  public void testStringify() {
    assertEquals("10 drop",
            (new SetGet(count, true, true)).stringify());
    assertEquals("10 delete",
            (new SetGet(count, true, false)).stringify());
    assertEquals("10",
            (new SetGet(count, false, true)).stringify());
    assertEquals("10",
            (new SetGet(count, false, false)).stringify());
  }
}
