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
package net.spy.memcached.collection.map;

import java.util.concurrent.TimeUnit;

import net.spy.memcached.collection.BaseIntegrationTest;
import net.spy.memcached.collection.CollectionAttributes;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

public class MopInsertWhenKeyNotExist extends BaseIntegrationTest {

  private String key = "MopInsertWhenKeyNotExist";

  private String[] items9 = {
      "value0", "value1", "value2", "value3",
      "value4", "value5", "value6", "value7", "value8"
  };

  @AfterEach
  @Override
  protected void tearDown() throws Exception {
    deleteMap(key);
    super.tearDown();
  }

  /**
   * <pre>
   * CREATE FIXED VALUE
   * true false null
   * </pre>
   */
  @Test
  public void testMopInsert_nokey_01() throws Exception {
    insertToFail(key, true, null);
  }

  /**
   * <pre>
   * CREATE FIXED VALUE
   * false  true  not null
   * </pre>
   */
  @Test
  public void testMopInsert_nokey_02() throws Exception {
    assertFalse(insertToSucceed(key, false, items9[0]));
  }

  /**
   * <pre>
   * CREATE FIXED VALUE
   * false  false not null
   * </pre>
   */
  @Test
  public void testMopInsert_nokey_04() throws Exception {
    assertFalse(insertToSucceed(key, false, items9[0]));
  }

  /**
   * <pre>
   * CREATE FIXED VALUE
   * true true  not null
   * </pre>
   */
  @Test
  public void testMopInsert_nokey_05() throws Exception {
    assertTrue(insertToSucceed(key, true, items9[0]));
  }

  boolean insertToFail(String key, boolean createKeyIfNotExists, Object value) {
    boolean result = false;
    try {
      result = mc
              .asyncMopInsert(
                      key,
                      "0",
                      value,
                      ((createKeyIfNotExists) ? new CollectionAttributes()
                              : null)).get(1000, TimeUnit.MILLISECONDS);
      fail("should be failed");
    } catch (Exception e) {
      // test success.
    }
    return result;
  }

  boolean insertToSucceed(String key, boolean createKeyIfNotExists,
                          Object value) {
    boolean result = false;
    try {
      result = mc
              .asyncMopInsert(
                      key,
                      "0",
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
