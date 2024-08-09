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
package net.spy.memcached.flushbyprefix;

import java.util.concurrent.TimeUnit;

import net.spy.memcached.collection.BaseIntegrationTest;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

public class FlushByPrefixTest extends BaseIntegrationTest {

  private final String PREFIX = "prefix";
  private final String DELIMITER = ":";
  private final String KEY = this.getClass().getSimpleName();
  private final String VALUE = "value";

  @Test
  public void testFlushByPrefix() {
    try {
      Boolean setResult = mc.set(PREFIX + DELIMITER + KEY, 60, VALUE)
              .get();
      assertTrue(setResult);
      Object object = mc.asyncGet(PREFIX + DELIMITER + KEY).get();
      assertEquals(VALUE, object);

      Boolean flushResult = mc.flush("prefix").get(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
      assertTrue(flushResult);

      Object object2 = mc.asyncGet(PREFIX + DELIMITER + KEY).get();
      assertNull(object2);
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void testFlushByPrefix1Depth() {
    try {
      for (int i = 0; i < 10; i++) {
        Boolean setResult = mc.set(PREFIX + DELIMITER + KEY + i, 60,
                VALUE).get();
        assertTrue(setResult);
        Object object = mc.asyncGet(PREFIX + DELIMITER + KEY + i).get();
        assertEquals(VALUE, object);
      }

      Boolean flushResult = mc.flush("prefix").get();
      assertTrue(flushResult);

      for (int i = 0; i < 10; i++) {
        Object object2 = mc.asyncGet(PREFIX + DELIMITER + KEY + i)
                .get();
        assertNull(object2);
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void testFlushByMultiPrefix() {
    try {
      for (int i = 0; i < 10; i++) {
        for (int prefix2 = 0; prefix2 < 10; prefix2++) {
          Boolean setResult = mc.set(
                  PREFIX + DELIMITER + prefix2 + DELIMITER + KEY + i,
                  60, VALUE).get();
          assertTrue(setResult);
          Object object = mc.asyncGet(
                  PREFIX + DELIMITER + prefix2 + DELIMITER + KEY + i)
                  .get();
          assertEquals(VALUE, object);
        }
      }

      Boolean flushResult = mc.flush("prefix").get();
      assertTrue(flushResult);

      for (int i = 0; i < 10; i++) {
        for (int prefix2 = 0; prefix2 < 10; prefix2++) {
          Object object2 = mc.asyncGet(
                  PREFIX + DELIMITER + prefix2 + DELIMITER + KEY + i)
                  .get();
          assertNull(object2);
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }
}
