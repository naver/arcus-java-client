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
package net.spy.memcached.bulkoperation;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.junit.Assert;
import net.spy.memcached.collection.BaseIntegrationTest;
import net.spy.memcached.ops.CollectionOperationStatus;

public class BulkSetVariousTypeTest extends BaseIntegrationTest {

  private static class MyBean implements Serializable {
    private static final long serialVersionUID = -5977830942924286134L;

    private String name;

    public MyBean(String name) {
      this.name = name;
    }

    @Override
    public boolean equals(Object obj) {
      if (obj instanceof MyBean) {
        return this.name.equals(((MyBean) obj).name);
      }
      return false;
    }
  }

  public void testInsertAndGet() {
    Object[] valueList = {1.0, 1000, 1000L, "String",
        new MyBean("beanName")};
    String keyPrefix = "TypeTestKey";

    try {
      for (int i = 0; i < valueList.length; i++) {
        String[] key = new String[]{keyPrefix + i};
        // REMOVE
        mc.delete(key[0]);

        // SET
        Future<Map<String, CollectionOperationStatus>> future = mc
                .asyncSetBulk(Arrays.asList(key), 60, valueList[i]);

        Map<String, CollectionOperationStatus> errorList;
        try {
          errorList = future.get(20000L, TimeUnit.MILLISECONDS);
          Assert.assertTrue("Error list is not empty.",
                  errorList.isEmpty());
        } catch (TimeoutException e) {
          future.cancel(true);
          Assert.fail(e.toString());
        }

        // GET
        Object v = mc.get(key[0]);
        Assert.assertEquals(String.format("K=%s, V=%s", key, v),
                valueList[i], v);
      }
    } catch (Exception e) {
      e.printStackTrace();
      Assert.fail();
    }
  }
}
