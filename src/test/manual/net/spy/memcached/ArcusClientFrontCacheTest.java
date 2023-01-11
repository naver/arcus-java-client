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
package net.spy.memcached;

import org.junit.Assert;
import junit.framework.TestCase;
import net.spy.memcached.collection.BaseIntegrationTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.runners.BlockJUnit4ClassRunner;
import org.junit.runner.RunWith;

import static org.junit.Assume.assumeTrue;

@RunWith(BlockJUnit4ClassRunner.class)
public class ArcusClientFrontCacheTest extends TestCase {

  @Before
  @Override
  public void setUp() throws Exception {
    super.setUp();
    // This test assumes we use ZK
    assumeTrue(BaseIntegrationTest.USE_ZK);
  }

  @Test
  public void testCreateSingleClient() {
    ConnectionFactoryBuilder cfb = new ConnectionFactoryBuilder();
    cfb.setFrontCacheExpireTime(10);
    cfb.setMaxFrontCacheElements(10);

    ArcusClient.createArcusClient(BaseIntegrationTest.ZK_HOST,
            BaseIntegrationTest.ZK_SERVICE_ID, cfb);
  }

  @Test
  public void testCreatePool() {
    ConnectionFactoryBuilder cfb = new ConnectionFactoryBuilder();
    cfb.setFrontCacheExpireTime(10);
    cfb.setMaxFrontCacheElements(10);

    ArcusClient.createArcusClientPool(BaseIntegrationTest.ZK_HOST,
            BaseIntegrationTest.ZK_SERVICE_ID, cfb, 4);
  }

  @Test
  public void testKV() {
    ConnectionFactoryBuilder cfb = new ConnectionFactoryBuilder();
    cfb.setFrontCacheExpireTime(10);
    cfb.setMaxFrontCacheElements(10);

    ArcusClient client = ArcusClient.createArcusClient(BaseIntegrationTest.ZK_HOST,
            BaseIntegrationTest.ZK_SERVICE_ID, cfb);

    try {
      Assert.assertTrue(client.set("test:key", 100, "value").get());
      Assert.assertEquals("value", client.get("test:key"));

      Assert.assertTrue(client.delete("test:key").get());

      Assert.assertNull(client.get("test:key"));
    } catch (Exception e) {
      Assert.fail(e.getMessage());
    }
  }
}
