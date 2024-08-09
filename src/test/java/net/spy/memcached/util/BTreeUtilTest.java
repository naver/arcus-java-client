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
package net.spy.memcached.util;

import java.util.Arrays;

import net.spy.memcached.compat.BaseMockCase;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class BTreeUtilTest extends BaseMockCase {

  @Test
  public void testAA() {

    System.out.println(Arrays.toString("".getBytes()));

  }

  @Test
  public void testFromByteArraysToHex() throws Exception {
    byte[] byteArray1 = {0, 'F', 'C', 0, 0};
    assertEquals("0x0046430000", BTreeUtil.toHex(byteArray1));

    byte[] byteArray2 = {0, 0, 1, 'C'};
    assertEquals("0x00000143", BTreeUtil.toHex(byteArray2));

    byte[] byteArray3 = {1, 'C', 'A', 0};
    assertEquals("0x01434100", BTreeUtil.toHex(byteArray3));

    byte[] byteArray4 = {0, 0, 'C', 'A', 0, 0};
    assertEquals("0x000043410000", BTreeUtil.toHex(byteArray4));
  }

  @Test
  public void testFromHexToByteArrays() throws Exception {
    byte[] byteArray1 = {0, 'F', 'C', 0, 0};
    assertTrue(Arrays.equals(byteArray1,
            BTreeUtil.hexStringToByteArrays("0x0046430000")));

    byte[] byteArray2 = {0, 0, 1, 'C'};
    assertTrue(Arrays.equals(byteArray2,
            BTreeUtil.hexStringToByteArrays("0x00000143")));

    byte[] byteArray3 = {1, 'C', 'A', 0};
    assertTrue(Arrays.equals(byteArray3,
            BTreeUtil.hexStringToByteArrays("0x01434100")));

    byte[] byteArray4 = {0, 0, 'C', 'A', 0, 0};
    assertTrue(Arrays.equals(byteArray4,
            BTreeUtil.hexStringToByteArrays("0x000043410000")));
  }

  @Test
  public void testCompareSameLengthByteArrays() throws Exception {
    byte[] array1 = {0, 0, 1, 0};
    byte[] array2 = {0, 0, 0, 0};
    byte[] array3 = {0, 0, 1, 0};

    assertEquals(1, BTreeUtil.compareByteArraysInLexOrder(array1, array2));
    assertEquals(-1, BTreeUtil.compareByteArraysInLexOrder(array2, array1));
    assertEquals(0, BTreeUtil.compareByteArraysInLexOrder(array1, array3));
  }

  @Test
  public void testCompareDifferentLengthByteArrays() throws Exception {
    byte[] array1 = {0, 0, 1};
    byte[] array2 = {0, 0, 1, 0};

    assertEquals(-1, BTreeUtil.compareByteArraysInLexOrder(array1, array2));
    assertEquals(1, BTreeUtil.compareByteArraysInLexOrder(array2, array1));
  }

  @Test
  public void testInValidSizeBkey() {
    assertThrows(IllegalArgumentException.class, () -> {
      BTreeUtil.validateBkey(new byte[32]);
    });
  }

  @Test
  public void testMinusLongBkey() {
    final long bkey = -1;
    assertThrows(IllegalArgumentException.class, () -> {
      BTreeUtil.validateBkey(bkey);
    });
  }
}
