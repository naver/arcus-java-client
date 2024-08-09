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
package net.spy.memcached.collection.btree;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import net.spy.memcached.collection.BTreeElement;
import net.spy.memcached.collection.BTreeGetResult;
import net.spy.memcached.collection.BaseIntegrationTest;
import net.spy.memcached.collection.CollectionAttributes;
import net.spy.memcached.collection.CollectionResponse;
import net.spy.memcached.collection.ElementFlagFilter;
import net.spy.memcached.internal.CollectionGetBulkFuture;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class BopGetBulkTest extends BaseIntegrationTest {

  private final List<String> keyList = new ArrayList<String>() {
    private static final long serialVersionUID = -4044682425313432602L;

    {
      add("BopGetBulkTest1");
      add("BopGetBulkTest2");
      add("BopGetBulkTest3");
      add("BopGetBulkTest4");
      add("BopGetBulkTest5");
    }
  };

  private final byte[] eFlag = {1, 8, 16, 32, 64};

  private final String value = String.valueOf(new Random().nextLong());

  @BeforeEach
  @Override
  protected void setUp() throws Exception {
    super.setUp();
    for (int i = 0; i < keyList.size(); i++) {
      mc.delete(keyList.get(i)).get();
      mc.asyncBopInsert(keyList.get(i), 0, null, value + "0",
              new CollectionAttributes()).get();
      mc.asyncBopInsert(keyList.get(i), 1, eFlag, value + "1",
              new CollectionAttributes()).get();
      mc.asyncBopInsert(keyList.get(i), 2, null, value + "2",
              new CollectionAttributes()).get();
    }
  }

  @Test
  public void testGetBulkLongBkeyGetAll() {
    try {
      ElementFlagFilter filter = ElementFlagFilter.DO_NOT_FILTER;

      CollectionGetBulkFuture<Map<String, BTreeGetResult<Long, Object>>> f = mc
              .asyncBopGetBulk(keyList, 0, 10, filter, 0, 10);

      Map<String, BTreeGetResult<Long, Object>> results = f.get(1000L,
              TimeUnit.MILLISECONDS);

      Assertions.assertEquals(keyList.size(), results.size());

      // System.out.println("\n\n\n");
      // for(Entry<String, BTreeGetResult<Long, Object>> entry :
      // results.entrySet()) {
      // System.out.println("\nk=" + entry.getKey());
      // System.out.println("code=" +
      // entry.getValue().getCollectionResponse().getMessage());
      //
      // if (entry.getValue().getElements() != null) {
      // for(Entry<Long, BTreeElement<Long, Object>> el :
      // entry.getValue().getElements().entrySet()) {
      // System.out.println("bkey=" + el.getKey() + ", eflag=" +
      // Arrays.toString(el.getValue().getEflag()) + ", value=" +
      // el.getValue().getValue());
      // }
      // }
      // }

      for (int i = 0; i < keyList.size(); i++) {
        BTreeGetResult<Long, Object> r = results.get(keyList.get(i));

        // check response
        Assertions.assertNotNull(r.getCollectionResponse().getResponse());
        // Assertions.assertEquals(CollectionResponse.OK,
        // r.getCollectionResponse().getResponse());

        // check elements
        Map<Long, BTreeElement<Long, Object>> elements = r
                .getElements();

        Assertions.assertEquals(3, elements.size());

        Assertions.assertTrue(Arrays.equals(eFlag, elements.get(1L)
                .getEflag()));

        for (long j = 0; j < elements.size(); j++) {
          Assertions.assertEquals(j, (long) elements.get(j).getBkey());
          Assertions.assertEquals(value + j, (String) elements.get(j)
                  .getValue());
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
      Assertions.fail(e.getMessage());
    }
  }

  @Test
  public void testGetBulkNotFoundAll() {
    try {
      for (int i = 0; i < keyList.size(); i++) {
        mc.delete(keyList.get(i)).get();
      }

      ElementFlagFilter filter = ElementFlagFilter.DO_NOT_FILTER;

      CollectionGetBulkFuture<Map<String, BTreeGetResult<Long, Object>>> f = mc
              .asyncBopGetBulk(keyList, 0, 10, filter, 0, 10);

      Map<String, BTreeGetResult<Long, Object>> results = f.get(1000L,
              TimeUnit.MILLISECONDS);

      Assertions.assertEquals(keyList.size(), results.size());

      // System.out.println("\n\n\n");
      // for(Entry<String, BTreeGetResult<Long, Object>> entry :
      // results.entrySet()) {
      // System.out.println("\nk=" + entry.getKey());
      // System.out.println("code=" +
      // entry.getValue().getCollectionResponse().getMessage());
      //
      // if (entry.getValue().getElements() != null) {
      // for(Entry<Long, BTreeElement<Long, Object>> el :
      // entry.getValue().getElements().entrySet()) {
      // System.out.println("bkey=" + el.getKey() + ", eflag=" +
      // Arrays.toString(el.getValue().getEflag()) + ", value=" +
      // el.getValue().getValue());
      // }
      // }
      // }

      for (int i = 0; i < keyList.size(); i++) {
        BTreeGetResult<Long, Object> r = results.get(keyList.get(i));

        Assertions.assertEquals(CollectionResponse.NOT_FOUND, r
                .getCollectionResponse().getResponse());
        Assertions.assertNull(r.getElements());
      }
    } catch (Exception e) {
      e.printStackTrace();
      Assertions.fail(e.getMessage());
    }
  }

  @Test
  public void testGetBulkNotFoundMixed() {
    try {
      // delete some data.
      for (int i = 0; i < keyList.size(); i++) {
        if (i % 2 == 0) {
          mc.delete(keyList.get(i)).get();
        }
      }

      ElementFlagFilter filter = ElementFlagFilter.DO_NOT_FILTER;

      CollectionGetBulkFuture<Map<String, BTreeGetResult<Long, Object>>> f = mc
              .asyncBopGetBulk(keyList, 0, 10, filter, 0, 10);

      Map<String, BTreeGetResult<Long, Object>> results = f.get(1000L,
              TimeUnit.MILLISECONDS);

      Assertions.assertEquals(keyList.size(), results.size());

      // for debug
      // System.out.println("\n\n\n");
      // for(Entry<String, BTreeGetResult<Long, Object>> entry :
      // results.entrySet()) {
      // System.out.println("\nk=" + entry.getKey());
      // System.out.println("code=" +
      // entry.getValue().getCollectionResponse().getMessage());
      //
      // if (entry.getValue().getElements() != null) {
      // for(Entry<Long, BTreeElement<Long, Object>> el :
      // entry.getValue().getElements().entrySet()) {
      // System.out.println("bkey=" + el.getKey() + ", eflag=" +
      // Arrays.toString(el.getValue().getEflag()) + ", value=" +
      // el.getValue().getValue());
      // }
      // }
      // }

      // check result
      for (int i = 0; i < keyList.size(); i++) {
        BTreeGetResult<Long, Object> r = results.get(keyList.get(i));

        if (i % 2 == 0) {
          Assertions.assertEquals(CollectionResponse.NOT_FOUND, r
                  .getCollectionResponse().getResponse());
        } else {
          Assertions.assertEquals(CollectionResponse.OK, r
                  .getCollectionResponse().getResponse());

          Map<Long, BTreeElement<Long, Object>> elements = r
                  .getElements();

          Assertions.assertEquals(3, elements.size());

          Assertions.assertTrue(Arrays.equals(eFlag, elements.get(1L)
                  .getEflag()));

          for (long j = 0; j < elements.size(); j++) {
            Assertions.assertEquals(j, (long) elements.get(j).getBkey());
            Assertions.assertEquals(value + j, (String) elements.get(j)
                    .getValue());
          }
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
      Assertions.fail(e.getMessage());
    }
  }

  @Test
  public void testErrorArguments() {
    try {
      Map<String, BTreeGetResult<Long, Object>> results = null;
      CollectionGetBulkFuture<Map<String, BTreeGetResult<Long, Object>>> f = null;

      // empty key list
      try {
        f = mc.asyncBopGetBulk(new ArrayList<>(), 0, 10,
                ElementFlagFilter.DO_NOT_FILTER, 0, 10);
        results = f.get(1000L, TimeUnit.MILLISECONDS);
      } catch (IllegalArgumentException e) {
        // test success
        Assertions.assertEquals("Key list is empty.", e.getMessage());
      }

      // max count list
      try {
        f = mc.asyncBopGetBulk(keyList, 0, 10,
                ElementFlagFilter.DO_NOT_FILTER, 0, 1000);
        results = f.get(1000L, TimeUnit.MILLISECONDS);
      } catch (IllegalArgumentException e) {
        // test success
      }
    } catch (Exception e) {
      e.printStackTrace();
      Assertions.fail(e.getMessage());
    }
  }

  @Test
  public void testUnreadable() {
    try {
      Map<String, BTreeGetResult<Long, Object>> results = null;
      CollectionGetBulkFuture<Map<String, BTreeGetResult<Long, Object>>> f = null;

      mc.delete(keyList.get(0)).get();
      CollectionAttributes attrs = new CollectionAttributes();
      attrs.setReadable(false);
      mc.asyncBopInsert(keyList.get(0), 0, null, value + "0", attrs)
              .get();

      f = mc.asyncBopGetBulk(keyList, 0, 10,
              ElementFlagFilter.DO_NOT_FILTER, 0, 10);
      results = f.get(1000L, TimeUnit.MILLISECONDS);

      Assertions.assertEquals(keyList.size(), results.size());
      Assertions.assertEquals("UNREADABLE", results.get(keyList.get(0))
              .getCollectionResponse().getMessage());
    } catch (Exception e) {
      Assertions.fail(e.getMessage());
    }
  }

  @Test
  public void testNotFoundElement() {
    try {
      Map<String, BTreeGetResult<Long, Object>> results = null;
      CollectionGetBulkFuture<Map<String, BTreeGetResult<Long, Object>>> f = null;

      mc.delete(keyList.get(0)).get();
      mc.asyncBopInsert(keyList.get(0), 0, null, value + "0",
              new CollectionAttributes()).get();
      mc.asyncBopInsert(keyList.get(0), 1, eFlag, value + "1",
              new CollectionAttributes()).get();
      mc.asyncBopInsert(keyList.get(0), 2, null, value + "2",
              new CollectionAttributes()).get();

      f = mc.asyncBopGetBulk(keyList, 1000, 10000,
              ElementFlagFilter.DO_NOT_FILTER, 0, 10);
      results = f.get(1000L, TimeUnit.MILLISECONDS);

      Assertions.assertEquals(keyList.size(), results.size());
      for (int i = 0; i < results.size(); i++) {
        Assertions.assertEquals("NOT_FOUND_ELEMENT",
                results.get(keyList.get(i)).getCollectionResponse()
                        .getMessage());
      }
    } catch (Exception e) {
      Assertions.fail(e.getMessage());
    }
  }

  @Test
  public void testTypeMismatch() {
    try {
      Map<String, BTreeGetResult<Long, Object>> results = null;
      CollectionGetBulkFuture<Map<String, BTreeGetResult<Long, Object>>> f = null;

      mc.delete(keyList.get(0)).get();
      mc.set(keyList.get(0), 10, "V").get(200L, TimeUnit.MILLISECONDS);

      f = mc.asyncBopGetBulk(keyList, 0, 10,
              ElementFlagFilter.DO_NOT_FILTER, 0, 10);
      results = f.get(1000L, TimeUnit.MILLISECONDS);

      Assertions.assertEquals(keyList.size(), results.size());
      Assertions.assertEquals("TYPE_MISMATCH", results.get(keyList.get(0))
              .getCollectionResponse().getMessage());
    } catch (Exception e) {
      Assertions.fail(e.getMessage());
    }
  }

  @Test
  public void testBKeyMismatch() {
    try {
      Map<String, BTreeGetResult<Long, Object>> results = null;
      CollectionGetBulkFuture<Map<String, BTreeGetResult<Long, Object>>> f = null;

      mc.delete(keyList.get(0)).get();
      mc.asyncBopInsert(keyList.get(0), new byte[]{0}, null,
              value + "0", new CollectionAttributes()).get();
      mc.asyncBopInsert(keyList.get(0), new byte[]{1}, eFlag,
              value + "0", new CollectionAttributes()).get();
      mc.asyncBopInsert(keyList.get(0), new byte[]{2}, null,
              value + "0", new CollectionAttributes()).get();

      f = mc.asyncBopGetBulk(keyList, 0, 10,
              ElementFlagFilter.DO_NOT_FILTER, 0, 10);
      results = f.get(1000L, TimeUnit.MILLISECONDS);

      Assertions.assertEquals(keyList.size(), results.size());
      Assertions.assertEquals("BKEY_MISMATCH", results.get(keyList.get(0))
              .getCollectionResponse().getMessage());
    } catch (Exception e) {
      Assertions.fail(e.getMessage());
    }
  }
}
