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

import net.spy.memcached.collection.ListDelete;

import org.junit.Assert;

public class ProtocolListDeleteTest extends TestCase {

  public void testStringfy() {
    // default setting : dropIfEmpty = true

    Assert.assertEquals("10 drop",
            (new ListDelete(10, false)).stringify());

    Assert.assertEquals("10",
            (new ListDelete(10, false, false)).stringify());
    Assert.assertEquals("10 drop",
            (new ListDelete(10, false, true)).stringify());

    Assert.assertEquals("10..20", (new ListDelete(10, 20, false,
            false)).stringify());
    Assert.assertEquals("10..20 drop", (new ListDelete(10, 20,
            false, true)).stringify());

    Assert.assertEquals("10 drop noreply",
            (new ListDelete(10, true)).stringify());

    Assert.assertEquals("10 noreply", (new ListDelete(10, true,
            false)).stringify());
    Assert.assertEquals("10 drop noreply", (new ListDelete(10,
            true, true)).stringify());

    Assert.assertEquals("10..20 noreply", (new ListDelete(10, 20,
            true, false)).stringify());
    Assert.assertEquals("10..20 drop noreply", (new ListDelete(10,
            20, true, true)).stringify());
  }
}
