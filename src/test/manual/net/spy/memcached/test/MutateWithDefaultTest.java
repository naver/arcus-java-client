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
package net.spy.memcached.test;

import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.junit.Assert;
import net.spy.memcached.collection.BaseIntegrationTest;

public class MutateWithDefaultTest extends BaseIntegrationTest {

  private String key = "MutateWithDefaultTest";

  @Override
  protected void setUp() throws Exception {
    super.setUp();
    mc.delete(key).get();
  }

  public void testIncr() {
    try {
      long v;
      Future<Long> future = mc.asyncIncr(key, 1);
      v = future.get(10000, TimeUnit.MILLISECONDS);
      assertEquals(v, -1);

      v = mc.incr(key, 1);
      assertEquals(v, -1);

      v = mc.incr(key, 10, 2);
      assertEquals(v, 2);

    } catch (Exception e) {
      e.printStackTrace();
      Assert.fail(e.getMessage());
    }
  }

  public void testDecr() {
    try {
      long v;

      Future<Long> future = mc.asyncDecr(key, 1);
      v = future.get(10000, TimeUnit.MILLISECONDS);
      assertEquals(v, -1);

      v = mc.decr(key, 1);
      assertEquals(v, -1);

      v = mc.decr(key, 10, 100);
      assertEquals(v, 100);

    } catch (Exception e) {
      e.printStackTrace();
      Assert.fail(e.getMessage());
    }
  }

  public void testIncrWithDefault() {
    try {

      Future<Long> future = mc.asyncIncr(key, 1, 100, 10);
      long v = future.get(10000, TimeUnit.MILLISECONDS);

      assertEquals(v, 100);

      Future<Long> future2 = mc.asyncIncr(key, 1, 100, 10);
      long v2 = future2.get(10000, TimeUnit.MILLISECONDS);

      assertEquals(v2, 101);

    } catch (Exception e) {
      e.printStackTrace();
      Assert.fail(e.getMessage());
    }
  }

  public void testDecrWithDefault() {
    try {

      Future<Long> future = mc.asyncDecr(key, 1, 100, 10);
      long v = future.get(10000, TimeUnit.MILLISECONDS);

      assertEquals(v, 100);

      Future<Long> future2 = mc.asyncDecr(key, 1, 100, 10);
      long v2 = future2.get(10000, TimeUnit.MILLISECONDS);

      assertEquals(v2, 99);

    } catch (Exception e) {
      e.printStackTrace();
      Assert.fail(e.getMessage());
    }
  }
}
