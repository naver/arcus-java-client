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
package net.spy.memcached.collection.list;

import java.util.concurrent.TimeUnit;

import net.spy.memcached.collection.BaseIntegrationTest;
import net.spy.memcached.collection.CollectionAttributes;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

public class LopInsertWhenKeyNotExist extends BaseIntegrationTest {

  private String key = "LopInsertWhenKeyNotExist";

  @AfterEach
  protected void tearDown() {
    try {
      deleteList(key, 1000);
      super.tearDown();
    } catch (Exception e) {
      // test success.
    }
  }

  /**
   * <pre>
   * INDEX  CREATE  FIXED VALUE
   * -1 true  false null
   * </pre>
   */
  @Test
  public void testLopInsert_nokey_01() throws Exception {
    insertToFail(key, -1, true, null);
  }

  /**
   * <pre>
   * INDEX  CREATE  FIXED VALUE
   * -1 false true  not null
   * </pre>
   */
  @Test
  public void testLopInsert_nokey_02() throws Exception {
    boolean success = insertToSucceed(key, -1, false, "some value");

    assertFalse(success);
  }

  /**
   * <pre>
   * INDEX  CREATE  FIXED VALUE
   * 0  false true  not null
   * </pre>
   */
  @Test
  public void testLopInsert_nokey_03() throws Exception {
    assertFalse(insertToSucceed(key, 0, false, "some value"));
  }

  /**
   * <pre>
   * INDEX  CREATE  FIXED VALUE
   * 0  false false not null
   * </pre>
   */
  @Test
  public void testLopInsert_nokey_04() throws Exception {
    assertFalse(insertToSucceed(key, 0, false, "some value"));
  }

  /**
   * <pre>
   * INDEX  CREATE  FIXED VALUE
   * 0  true  true  not null
   * </pre>
   */
  @Test
  public void testLopInsert_nokey_05() throws Exception {
    assertTrue(insertToSucceed(key, 0, true, "some value"));
  }

  /**
   * <pre>
   * INDEX  CREATE  FIXED VALUE
   * -1 true  false not null
   * </pre>
   */
  @Test
  public void testLopInsert_nokey_06() throws Exception {
    assertTrue(insertToSucceed(key, -1, true, "some value"));
  }

  /**
   * <pre>
   * INDEX  CREATE  FIXED VALUE
   * count  true  true  not null
   * </pre>
   */
  @Test
  public void testLopInsert_nokey_07() throws Exception {
    // Prepare 3 items
    String[] items = {"item01", "item02", "item03"};
    for (String item : items) {
      assertTrue(insertToSucceed(key, -1, true, item));
    }

    assertTrue(insertToSucceed(key, items.length, true, "item04"));
  }

  boolean insertToFail(String key, int index, boolean createKeyIfNotExists,
                       Object value) {
    boolean result = false;
    try {
      result = mc
              .asyncLopInsert(
                      key,
                      index,
                      value,
                      ((createKeyIfNotExists) ? new CollectionAttributes()
                              : null)).get(1000, TimeUnit.MILLISECONDS);
      fail("should be failed");
    } catch (Exception e) {
      // test success.
    }
    return result;
  }

  boolean insertToSucceed(String key, int index,
                          boolean createKeyIfNotExists, Object value) {
    boolean result = false;
    try {
      result = mc
              .asyncLopInsert(
                      key,
                      index,
                      value,
                      ((createKeyIfNotExists) ? new CollectionAttributes()
                              : null)).get(1000, TimeUnit.MILLISECONDS);
    } catch (Exception e) {
      e.printStackTrace();
      fail("should not be failed");
    }
    return result;
  }

}
