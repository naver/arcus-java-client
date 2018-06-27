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
package net.spy.memcached.collection;

import net.spy.memcached.collection.ElementFlagFilter.BitWiseOperands;
import net.spy.memcached.collection.ElementFlagFilter.CompOperands;
import junit.framework.Assert;
import junit.framework.TestCase;

public class ElementFlagFilterTest extends TestCase {

  public void testValidate1() {
    try {
      new ElementFlagFilter(null, null);
    } catch (NullPointerException e) {
      return;
    }
    Assert.fail("Argument validation failed.");
  }

  public void testValidate2() {
    try {
      new ElementFlagFilter(null, "".getBytes());
    } catch (NullPointerException e) {
      return;
    }
    Assert.fail("Argument validation failed.");
  }

  public void testValidate3() {
    try {
      new ElementFlagFilter(CompOperands.Equal, null);
    } catch (NullPointerException e) {
      return;
    }
    Assert.fail("Argument validation failed.");
  }

  public void testZeroLengthCompValue() {
    try {
      ElementFlagFilter filter = new ElementFlagFilter(
              CompOperands.Equal, "".getBytes());
      filter.toString();
    } catch (IllegalArgumentException e) {
      return;
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.toString());
    }
    fail("Oops. Something's going wrong.");
  }

  public void testZeroLengthBitCompValue() {
    try {
      ElementFlagFilter filter = new ElementFlagFilter(
              CompOperands.Equal, "A".getBytes());

      filter.setBitOperand(BitWiseOperands.AND, "".getBytes());
      filter.toString();
    } catch (IllegalArgumentException e) {
      return;
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.toString());
    }
    fail("Oops. Something's going wrong.");
  }

  public void testConstruct() {
    String src = "ABC";

    ElementFlagFilter filter = new ElementFlagFilter(CompOperands.Equal,
            src.getBytes());

    Assert.assertEquals("0 EQ 0x414243", filter.toString());

    filter.setCompareOffset(2);

    Assert.assertEquals("2 EQ 0x414243", filter.toString());

    filter.setBitOperand(BitWiseOperands.AND, src.getBytes());

    Assert.assertEquals("2 & 0x414243 EQ 0x414243", filter.toString());
  }

}
