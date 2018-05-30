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
package net.spy.memcached.collection.set;

import java.util.concurrent.ExecutionException;

import junit.framework.Assert;
import net.spy.memcached.collection.BaseIntegrationTest;
import net.spy.memcached.collection.CollectionAttributes;

public class SopDeleteTest extends BaseIntegrationTest {

  private static final String KEY = SopDeleteTest.class.getSimpleName();

  @Override
  protected void setUp() throws Exception {
    super.setUp();
    mc.delete(KEY).get();
    Assert.assertNull(mc.asyncGetAttr(KEY).get());
  }

  @Override
  protected void tearDown() throws Exception {
    mc.delete(KEY).get();
    super.tearDown();
  }

  public void testSopDelete0() throws InterruptedException,
          ExecutionException {
    testSopDelete(0);
  }

  public void testSopDelete1() throws InterruptedException,
          ExecutionException {
    testSopDelete(1);
  }

  public void testSopDelete(Object element) throws InterruptedException,
          ExecutionException {

    Assert.assertNull(mc.asyncSopGet(KEY, 100, false, true).get());

    Assert.assertTrue(mc.asyncSopInsert(KEY, element,
            new CollectionAttributes()).get());

    Assert.assertNotNull(mc.asyncSopGet(KEY, 100, false, true).get());

    Assert.assertTrue(mc.asyncSopDelete(KEY, element, true).get());

    Assert.assertNull(mc.asyncSopGet(KEY, 100, false, true).get());
  }

}
