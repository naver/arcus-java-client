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
package net.spy.memcached.emptycollection;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

import net.spy.memcached.collection.BaseIntegrationTest;
import net.spy.memcached.collection.CollectionAttributes;
import net.spy.memcached.collection.Element;
import net.spy.memcached.collection.ElementFlagFilter;
import net.spy.memcached.collection.ElementValueType;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class VariousTypeTest extends BaseIntegrationTest {

  private final String KEY = this.getClass().getSimpleName();
  private final long BKEY = 10;
  private final CollectionAttributes ATTR = new CollectionAttributes();

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
  public void testString() {
    try {
      String value = "VALUE";

      // create empty
      Boolean createResult = mc.asyncBopCreate(KEY,
              ElementValueType.STRING, ATTR).get();
      Assertions.assertTrue(createResult);

      // insert value
      Boolean insertResult = mc.asyncBopInsert(KEY, BKEY,
              ElementFlagFilter.EMPTY_ELEMENT_FLAG, value, ATTR).get();
      Assertions.assertTrue(insertResult);

      // get value
      Map<Long, Element<Object>> map = mc.asyncBopGet(KEY, BKEY,
              ElementFlagFilter.DO_NOT_FILTER, false, false).get();
      Assertions.assertEquals(value, map.get(BKEY).getValue());
    } catch (Exception e) {
      e.printStackTrace();
      Assertions.fail(e.getMessage());
    }
  }

  @Test
  public void testLong() {
    try {
      long value = 1234567890L;

      // create empty
      Boolean createResult = mc.asyncBopCreate(KEY,
              ElementValueType.LONG, ATTR).get();
      Assertions.assertTrue(createResult);

      // insert value
      Boolean insertResult = mc.asyncBopInsert(KEY, BKEY,
              ElementFlagFilter.EMPTY_ELEMENT_FLAG, value, ATTR).get();
      Assertions.assertTrue(insertResult);

      // get value
      Map<Long, Element<Object>> map = mc.asyncBopGet(KEY, BKEY,
              ElementFlagFilter.DO_NOT_FILTER, false, false).get();
      Assertions.assertEquals(value, map.get(BKEY).getValue());
    } catch (Exception e) {
      e.printStackTrace();
      Assertions.fail(e.getMessage());
    }
  }

  @Test
  public void testInteger() {
    try {
      int value = 1234567890;

      // create empty
      Boolean createResult = mc.asyncBopCreate(KEY,
              ElementValueType.INTEGER, ATTR).get();
      Assertions.assertTrue(createResult);

      // insert value
      Boolean insertResult = mc.asyncBopInsert(KEY, BKEY,
              ElementFlagFilter.EMPTY_ELEMENT_FLAG, value, ATTR).get();
      Assertions.assertTrue(insertResult);

      // get value
      Map<Long, Element<Object>> map = mc.asyncBopGet(KEY, BKEY,
              ElementFlagFilter.DO_NOT_FILTER, false, false).get();
      Assertions.assertEquals(value, map.get(BKEY).getValue());
    } catch (Exception e) {
      e.printStackTrace();
      Assertions.fail(e.getMessage());
    }
  }

  @Test
  public void testBoolean() {
    try {
      boolean value = false;

      // create empty
      Boolean createResult = mc.asyncBopCreate(KEY,
              ElementValueType.BOOLEAN, ATTR).get();
      Assertions.assertTrue(createResult);

      // insert value
      Boolean insertResult = mc.asyncBopInsert(KEY, BKEY,
              ElementFlagFilter.EMPTY_ELEMENT_FLAG, value, ATTR).get();
      Assertions.assertTrue(insertResult);

      // get value
      Map<Long, Element<Object>> map = mc.asyncBopGet(KEY, BKEY,
              ElementFlagFilter.DO_NOT_FILTER, false, false).get();
      Assertions.assertEquals(value, map.get(BKEY).getValue());
    } catch (Exception e) {
      e.printStackTrace();
      Assertions.fail(e.getMessage());
    }
  }

  @Test
  public void testDate() {
    try {
      Date value = new Date();

      // create empty
      Boolean createResult = mc.asyncBopCreate(KEY,
              ElementValueType.DATE, ATTR).get();
      Assertions.assertTrue(createResult);

      // insert value
      Boolean insertResult = mc.asyncBopInsert(KEY, BKEY,
              ElementFlagFilter.EMPTY_ELEMENT_FLAG, value, ATTR).get();
      Assertions.assertTrue(insertResult);

      // get value
      Map<Long, Element<Object>> map = mc.asyncBopGet(KEY, BKEY,
              ElementFlagFilter.DO_NOT_FILTER, false, false).get();
      Assertions.assertEquals(value, map.get(BKEY).getValue());
    } catch (Exception e) {
      e.printStackTrace();
      Assertions.fail(e.getMessage());
    }
  }

  @Test
  public void testByte() {
    try {
      byte value = 0x00;

      // create empty
      Boolean createResult = mc.asyncBopCreate(KEY,
              ElementValueType.BYTE, ATTR).get();
      Assertions.assertTrue(createResult);

      // insert value
      Boolean insertResult = mc.asyncBopInsert(KEY, BKEY,
              ElementFlagFilter.EMPTY_ELEMENT_FLAG, value, ATTR).get();
      Assertions.assertTrue(insertResult);

      // get value
      Map<Long, Element<Object>> map = mc.asyncBopGet(KEY, BKEY,
              ElementFlagFilter.DO_NOT_FILTER, false, false).get();
      Assertions.assertEquals(value, map.get(BKEY).getValue());
    } catch (Exception e) {
      e.printStackTrace();
      Assertions.fail(e.getMessage());
    }
  }

  @Test
  public void testFloat() {
    try {
      float value = 1234567890;

      // create empty
      Boolean createResult = mc.asyncBopCreate(KEY,
              ElementValueType.FLOAT, ATTR).get();
      Assertions.assertTrue(createResult);

      // insert value
      Boolean insertResult = mc.asyncBopInsert(KEY, BKEY,
              ElementFlagFilter.EMPTY_ELEMENT_FLAG, value, ATTR).get();
      Assertions.assertTrue(insertResult);

      // get value
      Map<Long, Element<Object>> map = mc.asyncBopGet(KEY, BKEY,
              ElementFlagFilter.DO_NOT_FILTER, false, false).get();
      Assertions.assertEquals(value, map.get(BKEY).getValue());
    } catch (Exception e) {
      e.printStackTrace();
      Assertions.fail(e.getMessage());
    }
  }

  @Test
  public void testDouble() {
    try {
      double value = 1234567890;

      // create empty
      Boolean createResult = mc.asyncBopCreate(KEY,
              ElementValueType.DOUBLE, ATTR).get();
      Assertions.assertTrue(createResult);

      // insert value
      Boolean insertResult = mc.asyncBopInsert(KEY, BKEY,
              ElementFlagFilter.EMPTY_ELEMENT_FLAG, value, ATTR).get();
      Assertions.assertTrue(insertResult);

      // get value
      Map<Long, Element<Object>> map = mc.asyncBopGet(KEY, BKEY,
              ElementFlagFilter.DO_NOT_FILTER, false, false).get();
      Assertions.assertEquals(value, map.get(BKEY).getValue());
    } catch (Exception e) {
      e.printStackTrace();
      Assertions.fail(e.getMessage());
    }
  }

  @Test
  public void testByteArray() {
    try {
      byte[] value = "value".getBytes();

      // create empty
      Boolean createResult = mc.asyncBopCreate(KEY,
              ElementValueType.BYTEARRAY, ATTR).get();
      Assertions.assertTrue(createResult);

      // insert value
      Boolean insertResult = mc.asyncBopInsert(KEY, BKEY,
              ElementFlagFilter.EMPTY_ELEMENT_FLAG, value, ATTR).get();
      Assertions.assertTrue(insertResult);

      // get value
      Map<Long, Element<Object>> map = mc.asyncBopGet(KEY, BKEY,
              ElementFlagFilter.DO_NOT_FILTER, false, false).get();
      Assertions.assertArrayEquals(value, (byte[]) map.get(BKEY)
          .getValue());
    } catch (Exception e) {
      e.printStackTrace();
      Assertions.fail(e.getMessage());
    }
  }

  @Test
  public void testOtherObjects() {
    try {
      UserDefinedClass value = new UserDefinedClass();

      // create empty
      Boolean createResult = mc.asyncBopCreate(KEY,
              ElementValueType.OTHERS, ATTR).get();
      Assertions.assertTrue(createResult);

      // insert value
      Boolean insertResult = mc.asyncBopInsert(KEY, BKEY,
              ElementFlagFilter.EMPTY_ELEMENT_FLAG, value, ATTR).get();
      Assertions.assertTrue(insertResult);

      // get value
      Map<Long, Element<Object>> map = mc.asyncBopGet(KEY, BKEY,
              ElementFlagFilter.DO_NOT_FILTER, false, false).get();
      Assertions.assertEquals(value, map.get(BKEY).getValue());
    } catch (Exception e) {
      e.printStackTrace();
      Assertions.fail(e.getMessage());
    }
  }

  @Test
  public void testList() {
    try {
      List<String> value = new ArrayList<>();
      value.add("Hello");
      value.add("Netspider");

      // create empty
      Boolean createResult = mc.asyncBopCreate(KEY,
              ElementValueType.OTHERS, ATTR).get();
      Assertions.assertTrue(createResult);

      // insert value
      Boolean insertResult = mc.asyncBopInsert(KEY, BKEY,
              ElementFlagFilter.EMPTY_ELEMENT_FLAG, value, ATTR).get();
      Assertions.assertTrue(insertResult);

      // get value
      Map<Long, Element<Object>> map = mc.asyncBopGet(KEY, BKEY,
              ElementFlagFilter.DO_NOT_FILTER, false, false).get();

      @SuppressWarnings("unchecked")
      List<String> r = (List<String>) map.get(BKEY).getValue();

      Assertions.assertEquals(2, r.size());
      Assertions.assertEquals("Hello", r.get(0));
      Assertions.assertEquals("Netspider", r.get(1));
    } catch (Exception e) {
      e.printStackTrace();
      Assertions.fail(e.getMessage());
    }
  }

  private static final class UserDefinedClass implements Serializable {
    private static final long serialVersionUID = 8942558579188233740L;

    private final int i;
    private final List<String> list;

    public UserDefinedClass() {
      this.i = 100;
      this.list = new ArrayList<>();
      this.list.add("Hello");
      this.list.add("Netspider");
    }

    public boolean equals(Object o) {
      if (!(o instanceof UserDefinedClass)) {
        return false;
      }

      UserDefinedClass c = (UserDefinedClass) o;

      if (this.i != c.i) {
        return false;
      }

      if (this.list == null && c.list == null) {
        return true;
      }

      if (this.list == null || c.list == null) {
        return false;
      }

      if (this.list.size() != c.list.size()) {
        return false;
      }

      for (int i = 0; i < this.list.size(); i++) {
        if (!this.list.get(i).equals(c.list.get(i))) {
          return false;
        }
      }

      return true;
    }

    @Override
    public int hashCode() {
      return toString().hashCode();
    }

    @Override
    public String toString() {
      return "UserDefinedClass{" +
          "i=" + i +
          ", list=" + list +
          '}';
    }
  }
}
