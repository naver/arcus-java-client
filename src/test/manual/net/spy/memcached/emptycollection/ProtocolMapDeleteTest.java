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

import net.spy.memcached.collection.MapDelete;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class ProtocolMapDeleteTest {

  @Test
  public void testStringify() {
    List<String> mkeyList = new ArrayList<>();
    mkeyList.add("mkey");

    List<String> mkeyList2 = new ArrayList<>();
    mkeyList2.add("mkey1");
    mkeyList2.add("mkey2");
    // default setting : dropIfEmpty = true

    MapDelete mapDelete;
    mapDelete = new MapDelete(mkeyList, false);
    mapDelete.setKeySeparator(" ");
    Assertions.assertEquals("4 1 drop", mapDelete.stringify());

    mapDelete = new MapDelete(mkeyList, false, false);
    mapDelete.setKeySeparator(" ");
    Assertions.assertEquals("4 1", mapDelete.stringify());
    mapDelete = new MapDelete(mkeyList, false, true);
    mapDelete.setKeySeparator(" ");
    Assertions.assertEquals("4 1 drop", mapDelete.stringify());

    mapDelete = new MapDelete(mkeyList2, false, false);
    mapDelete.setKeySeparator(" ");
    Assertions.assertEquals("11 2", mapDelete.stringify());
    mapDelete = new MapDelete(mkeyList2, false, true);
    mapDelete.setKeySeparator(" ");
    Assertions.assertEquals("11 2 drop", mapDelete.stringify());

    mapDelete = new MapDelete(mkeyList, true);
    mapDelete.setKeySeparator(" ");
    Assertions.assertEquals("4 1 drop noreply", mapDelete.stringify());

    mapDelete = new MapDelete(mkeyList, true, false);
    mapDelete.setKeySeparator(" ");
    Assertions.assertEquals("4 1 noreply", mapDelete.stringify());
    mapDelete = new MapDelete(mkeyList, true, true);
    mapDelete.setKeySeparator(" ");
    Assertions.assertEquals("4 1 drop noreply", mapDelete.stringify());

    mapDelete = new MapDelete(mkeyList2, true, false);
    mapDelete.setKeySeparator(" ");
    Assertions.assertEquals("11 2 noreply", mapDelete.stringify());
    mapDelete = new MapDelete(mkeyList2, true, true);
    mapDelete.setKeySeparator(" ");
    Assertions.assertEquals("11 2 drop noreply", mapDelete.stringify());
  }
}
