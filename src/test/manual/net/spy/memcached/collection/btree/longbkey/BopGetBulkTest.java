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
package net.spy.memcached.collection.btree.longbkey;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import junit.framework.Assert;
import net.spy.memcached.collection.BTreeElement;
import net.spy.memcached.collection.BTreeGetResult;
import net.spy.memcached.collection.BaseIntegrationTest;
import net.spy.memcached.collection.ByteArrayBKey;
import net.spy.memcached.collection.CollectionAttributes;
import net.spy.memcached.collection.CollectionResponse;
import net.spy.memcached.collection.ElementFlagFilter;
import net.spy.memcached.internal.CollectionGetBulkFuture;

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

  private final List<String> keyList2 = new ArrayList<String>() {
    private static final long serialVersionUID = -4044682425313432602L;

    {
      for (int i = 1; i < 500; i++) {
        add("BopGetBulkTest" + i);
      }
    }
  };

  private final byte[] eFlag = {1, 8, 16, 32, 64};

  private final String value = String.valueOf(new Random().nextLong());

  @Override
  protected void setUp() throws Exception {
    super.setUp();
    try {
      for (int i = 0; i < keyList.size(); i++) {
        mc.delete(keyList.get(i)).get();
        mc.asyncBopInsert(keyList.get(i), new byte[]{0}, null,
                value + "0", new CollectionAttributes()).get();
        mc.asyncBopInsert(keyList.get(i), new byte[]{1}, eFlag,
                value + "1", new CollectionAttributes()).get();
        mc.asyncBopInsert(keyList.get(i), new byte[]{2}, null,
                value + "2", new CollectionAttributes()).get();
      }
    } catch (Exception e) {
      e.printStackTrace();
      Assert.fail(e.getMessage());
    }

  }

  public void testGetBulkLongBkeyGetAll() {
    try {
      ElementFlagFilter filter = ElementFlagFilter.DO_NOT_FILTER;

      CollectionGetBulkFuture<Map<String, BTreeGetResult<ByteArrayBKey, Object>>> f = mc
              .asyncBopGetBulk(keyList, ByteArrayBKey.MIN,
                      ByteArrayBKey.MAX, filter, 0, 10);

      Map<String, BTreeGetResult<ByteArrayBKey, Object>> results = f.get(
              1000L, TimeUnit.MILLISECONDS);

      Assert.assertEquals(keyList.size(), results.size());

      // System.out.println("\n\n\n");
      // for(Entry<String, BTreeGetResult<ByteArrayBKey, Object>> entry :
      // results.entrySet()) {
      // System.out.println("\nk=" + entry.getKey());
      // System.out.println("code=" +
      // entry.getValue().getCollectionResponse().getMessage());
      //
      // if (entry.getValue().getElements() != null) {
      // for(Entry<ByteArrayBKey, BTreeElement<ByteArrayBKey, Object>> el
      // : entry.getValue().getElements().entrySet()) {
      // System.out.println("bkey=" + el.getKey() + ", eflag=" +
      // Arrays.toString(el.getValue().getEflag()) + ", value=" +
      // el.getValue().getValue());
      // }
      // }
      // }

      for (int i = 0; i < keyList.size(); i++) {
        BTreeGetResult<ByteArrayBKey, Object> r = results.get(keyList
                .get(i));

        // check response
        Assert.assertNotNull(r.getCollectionResponse().getResponse());
        // Assert.assertEquals(CollectionResponse.OK,
        // r.getCollectionResponse().getResponse());

        // check elements
        Map<ByteArrayBKey, BTreeElement<ByteArrayBKey, Object>> elements = r
                .getElements();

        Assert.assertEquals(3, elements.size());

        Assert.assertTrue(Arrays.equals(eFlag,
                elements.get(new byte[]{1}).getEflag()));

        for (long j = 0; j < elements.size(); j++) {
          Assert.assertTrue(Arrays.equals(new byte[]{(byte) j},
                  elements.get(new byte[]{(byte) j}).getBkey()
                          .getBytes()));
          Assert.assertEquals(value + j,
                  (String) elements.get(new byte[]{(byte) j})
                          .getValue());
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
      Assert.fail(e.getMessage());
    }
  }

  public void testGetBulkNotFoundAll() {
    try {
      for (int i = 0; i < keyList.size(); i++) {
        mc.delete(keyList.get(i)).get();
      }

      ElementFlagFilter filter = ElementFlagFilter.DO_NOT_FILTER;

      CollectionGetBulkFuture<Map<String, BTreeGetResult<ByteArrayBKey, Object>>> f = mc
              .asyncBopGetBulk(keyList, ByteArrayBKey.MIN,
                      ByteArrayBKey.MAX, filter, 0, 10);

      Map<String, BTreeGetResult<ByteArrayBKey, Object>> results = f.get(
              1000L, TimeUnit.MILLISECONDS);

      Assert.assertEquals(keyList.size(), results.size());

      // System.out.println("\n\n\n");
      // for(Entry<String, BTreeGetResult<ByteArrayBKey, Object>> entry :
      // results.entrySet()) {
      // System.out.println("\nk=" + entry.getKey());
      // System.out.println("code=" +
      // entry.getValue().getCollectionResponse().getMessage());
      //
      // if (entry.getValue().getElements() != null) {
      // for(Entry<ByteArrayBKey, BTreeElement<ByteArrayBKey, Object>> el
      // : entry.getValue().getElements().entrySet()) {
      // System.out.println("bkey=" + el.getKey() + ", eflag=" +
      // Arrays.toString(el.getValue().getEflag()) + ", value=" +
      // el.getValue().getValue());
      // }
      // }
      // }

      for (int i = 0; i < keyList.size(); i++) {
        BTreeGetResult<ByteArrayBKey, Object> r = results.get(keyList
                .get(i));

        Assert.assertEquals(CollectionResponse.NOT_FOUND, r
                .getCollectionResponse().getResponse());
        Assert.assertNull(r.getElements());
      }
    } catch (Exception e) {
      e.printStackTrace();
      Assert.fail(e.getMessage());
    }
  }

  public void testGetBulkNotFoundMixed() {
    try {
      // delete some data.
      for (int i = 0; i < keyList.size(); i++) {
        if (i % 2 == 0)
          mc.delete(keyList.get(i)).get();
      }

      ElementFlagFilter filter = ElementFlagFilter.DO_NOT_FILTER;

      CollectionGetBulkFuture<Map<String, BTreeGetResult<ByteArrayBKey, Object>>> f = mc
              .asyncBopGetBulk(keyList, ByteArrayBKey.MIN,
                      ByteArrayBKey.MAX, filter, 0, 10);

      Map<String, BTreeGetResult<ByteArrayBKey, Object>> results = f.get(
              1000L, TimeUnit.MILLISECONDS);

      Assert.assertEquals(keyList.size(), results.size());

      // System.out.println("\n\n\n");
      // for(Entry<String, BTreeGetResult<ByteArrayBKey, Object>> entry :
      // results.entrySet()) {
      // System.out.println("\nk=" + entry.getKey());
      // System.out.println("code=" +
      // entry.getValue().getCollectionResponse().getMessage());
      //
      // if (entry.getValue().getElements() != null) {
      // for(Entry<ByteArrayBKey, BTreeElement<ByteArrayBKey, Object>> el
      // : entry.getValue().getElements().entrySet()) {
      // System.out.println("bkey=" + el.getKey() + ", eflag=" +
      // Arrays.toString(el.getValue().getEflag()) + ", value=" +
      // el.getValue().getValue());
      // }
      // }
      // }

      // check result
      for (int i = 0; i < keyList.size(); i++) {
        BTreeGetResult<ByteArrayBKey, Object> r = results.get(keyList
                .get(i));

        if (i % 2 == 0) {
          Assert.assertEquals(CollectionResponse.NOT_FOUND, r
                  .getCollectionResponse().getResponse());
        } else {
          Assert.assertEquals(CollectionResponse.OK, r
                  .getCollectionResponse().getResponse());

          Map<ByteArrayBKey, BTreeElement<ByteArrayBKey, Object>> elements = r
                  .getElements();

          Assert.assertEquals(3, elements.size());

          Assert.assertTrue(Arrays.equals(eFlag,
                  elements.get(new byte[]{1}).getEflag()));

          for (long j = 0; j < elements.size(); j++) {
            Assert.assertTrue(Arrays.equals(
                    new byte[]{(byte) j},
                    elements.get(new byte[]{(byte) j}).getBkey()
                            .getBytes()));
            Assert.assertEquals(value + j,
                    (String) elements.get(new byte[]{(byte) j})
                            .getValue());
          }
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
      Assert.fail(e.getMessage());
    }
  }

  public void testErrorArguments() {
    try {
      Map<String, BTreeGetResult<ByteArrayBKey, Object>> results = null;
      CollectionGetBulkFuture<Map<String, BTreeGetResult<ByteArrayBKey, Object>>> f = null;

      // empty key list
      f = mc.asyncBopGetBulk(new ArrayList<String>(), new byte[]{0},
              new byte[]{10}, ElementFlagFilter.DO_NOT_FILTER, 0, 10);
      results = f.get(1000L, TimeUnit.MILLISECONDS);
      Assert.assertEquals(0, results.size());

      // max key list
      try {
        f = mc.asyncBopGetBulk(keyList2, new byte[]{0},
                new byte[]{10}, ElementFlagFilter.DO_NOT_FILTER, 0,
                10);
        results = f.get(1000L, TimeUnit.MILLISECONDS);
      } catch (IllegalArgumentException e) {
        // test success
      }

      // max count list
      try {
        f = mc.asyncBopGetBulk(keyList, new byte[]{0},
                new byte[]{10}, ElementFlagFilter.DO_NOT_FILTER, 0,
                1000);
        results = f.get(1000L, TimeUnit.MILLISECONDS);
      } catch (IllegalArgumentException e) {
        // test success
      }
    } catch (Exception e) {
      e.printStackTrace();
      Assert.fail(e.getMessage());
    }
  }

  public void testUnreadable() {
    try {
      Map<String, BTreeGetResult<ByteArrayBKey, Object>> results = null;
      CollectionGetBulkFuture<Map<String, BTreeGetResult<ByteArrayBKey, Object>>> f = null;

      mc.delete(keyList.get(0)).get();
      CollectionAttributes attrs = new CollectionAttributes();
      attrs.setReadable(false);
      mc.asyncBopInsert(keyList.get(0), new byte[]{0}, null,
              value + "0", attrs).get();

      f = mc.asyncBopGetBulk(keyList, new byte[]{0},
              new byte[]{10}, ElementFlagFilter.DO_NOT_FILTER, 0, 10);
      results = f.get(1000L, TimeUnit.MILLISECONDS);

      Assert.assertEquals(keyList.size(), results.size());
      Assert.assertEquals("UNREADABLE", results.get(keyList.get(0))
              .getCollectionResponse().getMessage());
    } catch (Exception e) {
      Assert.fail(e.getMessage());
    }
  }

  public void testNotFoundElement() {
    try {
      Map<String, BTreeGetResult<ByteArrayBKey, Object>> results = null;
      CollectionGetBulkFuture<Map<String, BTreeGetResult<ByteArrayBKey, Object>>> f = null;

      mc.delete(keyList.get(0)).get();
      mc.asyncBopInsert(keyList.get(0), new byte[]{0}, null,
              value + "0", new CollectionAttributes()).get();
      mc.asyncBopInsert(keyList.get(0), new byte[]{1}, eFlag,
              value + "1", new CollectionAttributes()).get();
      mc.asyncBopInsert(keyList.get(0), new byte[]{2}, null,
              value + "2", new CollectionAttributes()).get();

      f = mc.asyncBopGetBulk(keyList, new byte[]{32},
              new byte[]{64}, ElementFlagFilter.DO_NOT_FILTER, 0, 10);
      results = f.get(1000L, TimeUnit.MILLISECONDS);

      Assert.assertEquals(keyList.size(), results.size());
      for (int i = 0; i < results.size(); i++) {
        Assert.assertEquals("NOT_FOUND_ELEMENT",
                results.get(keyList.get(i)).getCollectionResponse()
                        .getMessage());
      }
    } catch (Exception e) {
      Assert.fail(e.getMessage());
    }
  }

  public void testTypeMismatch() {
    try {
      Map<String, BTreeGetResult<ByteArrayBKey, Object>> results = null;
      CollectionGetBulkFuture<Map<String, BTreeGetResult<ByteArrayBKey, Object>>> f = null;

      mc.delete(keyList.get(0)).get();
      mc.set(keyList.get(0), 10, "V").get(200L, TimeUnit.MILLISECONDS);

      f = mc.asyncBopGetBulk(keyList, new byte[]{0},
              new byte[]{10}, ElementFlagFilter.DO_NOT_FILTER, 0, 10);
      results = f.get(1000L, TimeUnit.MILLISECONDS);

      Assert.assertEquals(keyList.size(), results.size());
      Assert.assertEquals("TYPE_MISMATCH", results.get(keyList.get(0))
              .getCollectionResponse().getMessage());
    } catch (Exception e) {
      Assert.fail(e.getMessage());
    }
  }

  public void testBKeyMismatch() {
    try {
      Map<String, BTreeGetResult<ByteArrayBKey, Object>> results = null;
      CollectionGetBulkFuture<Map<String, BTreeGetResult<ByteArrayBKey, Object>>> f = null;

      mc.delete(keyList.get(0)).get();
      mc.asyncBopInsert(keyList.get(0), 0, null, value + "0",
              new CollectionAttributes()).get();
      mc.asyncBopInsert(keyList.get(0), 1, eFlag, value + "0",
              new CollectionAttributes()).get();
      mc.asyncBopInsert(keyList.get(0), 2, null, value + "0",
              new CollectionAttributes()).get();

      f = mc.asyncBopGetBulk(keyList, new byte[]{0},
              new byte[]{10}, ElementFlagFilter.DO_NOT_FILTER, 0, 10);
      results = f.get(1000L, TimeUnit.MILLISECONDS);

      Assert.assertEquals(keyList.size(), results.size());
      Assert.assertEquals("BKEY_MISMATCH", results.get(keyList.get(0))
              .getCollectionResponse().getMessage());
    } catch (Exception e) {
      Assert.fail(e.getMessage());
    }
  }
}
