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

import java.util.ArrayList;
import java.util.List;

import net.spy.memcached.collection.MapGet;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class ProtocolMapGetTest {

  private final String MKEY = "mkey";

  @Test
  public void testStringify() {
    // default setting : dropIfEmpty = true

    List<String> mkeyList = new ArrayList<>();
    mkeyList.add(MKEY);
    Assertions.assertEquals("4 1 drop",
            (new MapGet(mkeyList, true, true)).stringify());
    Assertions.assertEquals("4 1 delete",
            (new MapGet(mkeyList, true, false)).stringify());
    Assertions.assertEquals("4 1",
            (new MapGet(mkeyList, false, true)).stringify());
    Assertions.assertEquals("4 1",
            (new MapGet(mkeyList, false, false)).stringify());
  }
}
