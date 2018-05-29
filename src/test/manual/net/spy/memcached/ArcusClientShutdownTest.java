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

import junit.framework.Assert;
import net.spy.memcached.collection.BaseIntegrationTest;

import org.junit.Ignore;

@Ignore
public class ArcusClientShutdownTest extends BaseIntegrationTest {

  @Override
  protected void setUp() throws Exception {
  }

  @Override
  protected void tearDown() throws Exception {
  }

  public void testOpenAndWait() {
    if (!USE_ZK) {
      return;
    }

    ConnectionFactoryBuilder cfb = new ConnectionFactoryBuilder();
    ArcusClient client = ArcusClient.createArcusClient(ZK_HOST,
            ZK_SERVICE_ID, cfb);

    // This threads must be stopped after client is shutdown.
    List<String> threadNames = new ArrayList<String>();
    threadNames.add("main-EventThread");
    threadNames.add("main-SendThread(" + ZK_HOST + ")");
    threadNames
            .add("Cache Manager IO for " + ZK_SERVICE_ID + "@" + ZK_HOST);

    // Check exists threads
    List<String> currentThreads = new ArrayList<String>();
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
