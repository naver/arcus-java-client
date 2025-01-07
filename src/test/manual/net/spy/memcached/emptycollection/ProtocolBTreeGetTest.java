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

import net.spy.memcached.collection.BTreeGet;
import net.spy.memcached.collection.ElementFlagFilter;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ProtocolBTreeGetTest {

  private static final long bkey = 10;

  @Test
  void testStringify() {
    assertEquals("10 drop", (new BTreeGet(bkey, ElementFlagFilter.DO_NOT_FILTER,
            true, true)).stringify());
    assertEquals("10 delete", (new BTreeGet(bkey, ElementFlagFilter.DO_NOT_FILTER,
            true, false)).stringify());
    assertEquals("10", (new BTreeGet(bkey, ElementFlagFilter.DO_NOT_FILTER,
            false, true)).stringify());
    assertEquals("10", (new BTreeGet(bkey, ElementFlagFilter.DO_NOT_FILTER,
            false, false)).stringify());

    assertEquals("10..20 1 1 delete", (new BTreeGet(10, 20,
            ElementFlagFilter.DO_NOT_FILTER,
            1, 1, true, false))
            .stringify());
    assertEquals("10..20 1 1 drop", (new BTreeGet(10, 20,
            ElementFlagFilter.DO_NOT_FILTER, 1,
            1, true, true)).stringify());
    assertEquals("10..20 1 1", (new BTreeGet(10, 20, ElementFlagFilter.DO_NOT_FILTER,
            1, 1, false, true)).stringify());
    assertEquals("10..20 1 1", (new BTreeGet(10, 20, ElementFlagFilter.DO_NOT_FILTER,
            1, 1, false, false)).stringify());
  }
}
