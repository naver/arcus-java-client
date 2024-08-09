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

import net.spy.memcached.collection.BaseIntegrationTest;
import net.spy.memcached.collection.CollectionAttributes;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class SopDeleteTest extends BaseIntegrationTest {

  private static final String KEY = SopDeleteTest.class.getSimpleName();

  @BeforeEach
  @Override
  protected void setUp() throws Exception {
    super.setUp();
    mc.delete(KEY).get();
    Assertions.assertNull(mc.asyncGetAttr(KEY).get());
  }

  @AfterEach
  @Override
  protected void tearDown() throws Exception {
    mc.delete(KEY).get();
    super.tearDown();
  }

  @Test
  public void testSopDelete0() throws InterruptedException,
          ExecutionException {
    testSopDelete(0);
  }

  @Test
  public void testSopDelete1() throws InterruptedException,
          ExecutionException {
    testSopDelete(1);
  }

  public void testSopDelete(Object element) throws InterruptedException,
          ExecutionException {

    Assertions.assertNull(mc.asyncSopGet(KEY, 100, false, true).get());

    Assertions.assertTrue(mc.asyncSopInsert(KEY, element,
            new CollectionAttributes()).get());

    Assertions.assertNotNull(mc.asyncSopGet(KEY, 100, false, true).get());

    Assertions.assertTrue(mc.asyncSopDelete(KEY, element, true).get());

    Assertions.assertNull(mc.asyncSopGet(KEY, 100, false, true).get());
  }

}
