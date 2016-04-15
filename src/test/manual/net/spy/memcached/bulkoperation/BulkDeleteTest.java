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
package net.spy.memcached.bulkoperation;

import junit.framework.Assert;
import net.spy.memcached.collection.BaseIntegrationTest;
import net.spy.memcached.ops.CollectionOperationStatus;

import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;


public class BulkDeleteTest extends BaseIntegrationTest {

    public void testBulkDelete() {
        String value = "MyValue";

        int keyCount = 500;

        try {
            // generate key
            String[] keys = new String[keyCount];
            for (int i = 0; i < keys.length; i++) {
                keys[i] = "MyKey" + i;
            }

            // REMOVE
            for (String key : keys) {
                mc.delete(key);
            }

            // SET
            Future<Map<String, CollectionOperationStatus>> future = mc
                    .asyncSetBulk(Arrays.asList(keys), 60, value);

            Map<String, CollectionOperationStatus> errorList;

            try {
                errorList = future.get(20000L, TimeUnit.MILLISECONDS);
                Assert.assertTrue("BulkSet Error list is not empty.",
                        errorList.isEmpty());
            } catch (TimeoutException e) {
                future.cancel(true);
                e.printStackTrace();
            }

            // Bulk Delete
            future = mc.asyncDeleteBulk(Arrays.asList(keys));

            try {
                errorList = future.get(20000L, TimeUnit.MILLISECONDS);
                Assert.assertTrue("BulkDelete Error list is not empty.",
                        errorList.isEmpty());
            } catch (TimeoutException e) {
                future.cancel(true);
                e.printStackTrace();
            }

            // GET
            int errorCount = 0;
            for (String key : keys) {
                String v = (String) mc.get(key);
                if (v != null) {
                    errorCount++;
                }
            }

            Assert.assertEquals("Error count is greater than 0.", 0,
                    errorCount);


        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

}
