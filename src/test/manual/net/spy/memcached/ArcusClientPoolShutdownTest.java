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

import java.util.ArrayList;
import java.util.List;

import junit.framework.TestCase;

import net.spy.memcached.collection.BaseIntegrationTest;

import org.junit.Assert;
import org.junit.Ignore;

@Ignore
public class ArcusClientPoolShutdownTest extends TestCase {

  public void testOpenAndWait() {
    if (!BaseIntegrationTest.USE_ZK) {
      return;
    }

    ArcusClientPool client = ArcusClient.createArcusClientPool(BaseIntegrationTest.ZK_HOST,
            BaseIntegrationTest.ZK_SERVICE_ID, 2);

    // This threads must be stopped after client is shutdown.
    List<String> threadNames = new ArrayList<>();
    threadNames.add("main-EventThread");
    threadNames.add("main-SendThread(" + BaseIntegrationTest.ZK_HOST + ")");
    threadNames
            .add("Cache Manager IO for " + BaseIntegrationTest.ZK_SERVICE_ID +
                    "@" + BaseIntegrationTest.ZK_HOST);

    // Check exists threads
    List<String> currentThreads = new ArrayList<>();
    for (Thread t : Thread.getAllStackTraces().keySet()) {
      currentThreads.add(t.getName());
    }
    for (String name : threadNames) {
      Assert.assertTrue(currentThreads.contains(name));
    }

    // Sleep 1s
    try {
      Thread.sleep(1000L);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }

    // Shutdown the client.
    client.shutdown();

    // Sleep 1s
    try {
      Thread.sleep(1000L);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }

    // Check the threads after shutdown the client
    currentThreads.clear();
    for (Thread t : Thread.getAllStackTraces().keySet()) {
      currentThreads.add(t.getName());
    }
    for (String name : threadNames) {
      Assert.assertTrue("Thread '" + name + "' is exists.",
              !currentThreads.contains(name));
    }
  }
}
