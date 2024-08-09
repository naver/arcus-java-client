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

import net.spy.memcached.collection.ListGet;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class ProtocolListGetTest {

  private static final int index = 10;

  @Test
  public void testStringify() {
    Assertions.assertEquals("10 drop",
            (new ListGet(index, true, true)).stringify());
    Assertions.assertEquals("10 delete", (new ListGet(index, true,
            false)).stringify());
    Assertions.assertEquals("10",
            (new ListGet(index, false, true)).stringify());
    Assertions.assertEquals("10",
            (new ListGet(index, false, false)).stringify());

    Assertions.assertEquals("10..20 delete", (new ListGet(10, 20, true,
            false)).stringify());
    Assertions.assertEquals("10..20 drop", (new ListGet(10, 20, true,
            true)).stringify());
    Assertions.assertEquals("10..20",
            (new ListGet(10, 20, false, true)).stringify());
    Assertions.assertEquals("10..20",
            (new ListGet(10, 20, false, false)).stringify());
  }
}
