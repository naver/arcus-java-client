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

import net.spy.memcached.collection.BaseIntegrationTest;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

@Disabled
public class ArcusClientPoolReconnectTest {

  @Test
  public void testOpenAndWait() {
    ArcusClientPool client = ArcusClient.createArcusClientPool(BaseIntegrationTest.ZK_ADDRESS,
            BaseIntegrationTest.SERVICE_CODE, 2);

    try {
      Thread.sleep(120000L);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }

    client.shutdown();

    try {
      Thread.sleep(Long.MAX_VALUE);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }
}
