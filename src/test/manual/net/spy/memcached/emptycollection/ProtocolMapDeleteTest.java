/*
 * arcus-java-client : Arcus Java client
 * Copyright 2016 JaM2in Co., Ltd.
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

import junit.framework.Assert;
import junit.framework.TestCase;
import net.spy.memcached.collection.MapDelete;

import java.util.List;
import java.util.ArrayList;

public class ProtocolMapDeleteTest extends TestCase {

  public void testStringfy() {
    List<String> mkeyList = new ArrayList<String>();
    mkeyList.add("mkey");

    List<String> mkeyList2 = new ArrayList<String>();
    mkeyList2.add("mkey1");
    mkeyList2.add("mkey2");
    // default setting : dropIfEmpty = true

    Assert.assertEquals("4 1 drop",
            (new MapDelete(mkeyList, false)).stringify());

    Assert.assertEquals("4 1",
            (new MapDelete(mkeyList, false, false)).stringify());
    Assert.assertEquals("4 1 drop",
            (new MapDelete(mkeyList, false, true)).stringify());

    Assert.assertEquals("11 2", (new MapDelete(mkeyList2, false,
            false)).stringify());
    Assert.assertEquals("11 2 drop", (new MapDelete(mkeyList2,
            false, true)).stringify());

    Assert.assertEquals("4 1 drop noreply",
            (new MapDelete(mkeyList, true)).stringify());

    Assert.assertEquals("4 1 noreply", (new MapDelete(mkeyList, true,
            false)).stringify());
    Assert.assertEquals("4 1 drop noreply", (new MapDelete(mkeyList,
            true, true)).stringify());

    Assert.assertEquals("11 2 noreply", (new MapDelete(mkeyList2,
            true, false)).stringify());
    Assert.assertEquals("11 2 drop noreply", (new MapDelete(mkeyList2,
            true, true)).stringify());
  }
}