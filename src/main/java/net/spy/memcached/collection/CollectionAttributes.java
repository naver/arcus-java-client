/*
 * arcus-java-client : Arcus Java client
 * Copyright 2010-2014 NAVER Corp.
 * Copyright 2014-2022 JaM2in Co., Ltd.
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

import net.spy.memcached.util.BTreeUtil;

public class CollectionAttributes extends Attributes {

  public static final Long DEFAULT_MAXCOUNT = 4000L;
  public static final CollectionOverflowAction DEFAULT_OVERFLOWACTION =
      CollectionOverflowAction.tail_trim;

  private Long count;
  private Long maxCount;
  private CollectionOverflowAction overflowAction;
  private Boolean readable;

  //b+tree only attribute
  private BKeyObject maxBkeyRangeObject = null;
  private BKeyObject minBkeyObject = null;
  private BKeyObject maxBkeyObject = null;
  private Long trimmed = null;

  private String stringCache;

  public CollectionAttributes() {
  }

  public CollectionAttributes(Integer expireTime,
                              Long maxCount, CollectionOverflowAction overflowAction) {
    this.expireTime = expireTime;
    this.maxCount = maxCount;
    this.overflowAction = overflowAction;
  }

  protected String stringify() {
    StringBuilder b = new StringBuilder();

    if (flags != null) {
      b.append(" flags=").append(flags);
    }
    if (expireTime != null) {
      b.append(" expiretime=").append(expireTime);
    }
    if (type != null) {
      b.append(" type=").append(type.getStringValue());
    }
    if (count != null) {
      b.append(" count=").append(count);
    }
    if (maxCount != null) {
      b.append(" maxcount=").append(maxCount);
    }
    if (overflowAction != null) {
      b.append(" overflowaction=").append(String.valueOf(overflowAction));
    }
    if (readable != null) {
      b.append(" readable=").append((readable) ? "on" : "off");
    }
    if (maxBkeyRangeObject != null) {
      b.append(" maxbkeyrange=").append(maxBkeyRangeObject);
    }

    stringCache = (b.length() < 1) ? "" : b.substring(1);
    return stringCache;
  }

  @Override
  public String toString() {
    return (stringCache == null) ? stringify() : stringCache;
  }

  public int getLength() {
    return (stringCache == null) ? stringify().length() : stringCache.length();
  }

  public void setAttribute(String attribute) {
    String[] splited = attribute.split("=");
    assert splited.length == 2 : "An attribute should be given in \"name=value\" format.";

    String name = splited[0];
    String value = splited[1];

    try {
      if ("flags".equals(name)) {
        flags = Integer.parseInt(value);
      } else if ("expiretime".equals(name)) {
        expireTime = Integer.parseInt(value);
      } else if ("type".equals(name)) {
        type = CollectionType.find(value);
      } else if ("count".equals(name)) {
        count = Long.parseLong(value);
      } else if ("maxcount".equals(name)) {
        maxCount = Long.parseLong(value);
      } else if ("overflowaction".equals(name)) {
        overflowAction = CollectionOverflowAction.valueOf(value);
      } else if ("readable".equals(name)) {
        readable = "on".equals(value);
      } else if ("maxbkeyrange".equals(name)) {
        if (value.startsWith("0x")) {
          maxBkeyRangeObject = new BKeyObject(
                  BTreeUtil.hexStringToByteArrays(value.substring(2)));
        } else {
          maxBkeyRangeObject = new BKeyObject(Long.parseLong(value));
        }
      } else if ("minbkey".equals(name)) {
        if (!value.startsWith("-1")) {
          if (value.startsWith("0x")) {
            minBkeyObject = new BKeyObject(
                    BTreeUtil.hexStringToByteArrays(value.substring(2)));
          } else {
            minBkeyObject = new BKeyObject(Long.parseLong(value));
          }
        }
      } else if ("maxbkey".equals(name)) {
        if (!value.startsWith("-1")) {
          if (value.startsWith("0x")) {
            maxBkeyObject = new BKeyObject(
                    BTreeUtil.hexStringToByteArrays(value.substring(2)));
          } else {
            maxBkeyObject = new BKeyObject(Long.parseLong(value));
          }
        }
      } else if ("trimmed".equals(name)) {
        trimmed = Long.parseLong(value);
      }
    } catch (Exception e) {
      getLogger().info(e, e);
      assert false : "Unexpected value.";
    }
  }

  private void clearCache() {
    this.stringCache = null;
  }

  public void setMaxCount(long maxCount) {
    clearCache();
    this.maxCount = maxCount;
  }

  public void setOverflowAction(CollectionOverflowAction overflowAction) {
    clearCache();
    this.overflowAction = overflowAction;
  }

  public void setReadable(Boolean readable) {
    clearCache();
    this.readable = readable;
  }

  public Long getCount() {
    return count;
  }

  public Long getMaxCount() {
    return maxCount;
  }

  public CollectionOverflowAction getOverflowAction() {
    return overflowAction;
  }

  public Boolean getReadable() {
    return readable;
  }

  public Long getMaxBkeyRange() {
    if (maxBkeyRangeObject == null) {
      return null;
    }
    return maxBkeyRangeObject.getLongBKey();
  }

  public void setMaxBkeyRange(Long maxBkeyRange) {
    clearCache();
    this.maxBkeyRangeObject = new BKeyObject(maxBkeyRange);
  }

  public byte[] getMaxBkeyRangeByBytes() {
    if (maxBkeyRangeObject == null) {
      return null;
    }
    return maxBkeyRangeObject.getByteArrayBKeyRaw();
  }

  public void setMaxBkeyRangeByBytes(byte[] maxBkeyRangeByBytes) {
    clearCache();
    this.maxBkeyRangeObject = new BKeyObject(maxBkeyRangeByBytes);
  }

  public Long getMinBkey() {
    if (minBkeyObject == null) {
      return null;
    }
    return minBkeyObject.getLongBKey();
  }

  public byte[] getMinBkeyByBytes() {
    if (minBkeyObject == null) {
      return null;
    }
    return minBkeyObject.getByteArrayBKeyRaw();
  }

  public Long getMaxBkey() {
    if (maxBkeyObject == null) {
      return null;
    }
    return maxBkeyObject.getLongBKey();
  }

  public byte[] getMaxBkeyByBytes() {
    if (maxBkeyObject == null) {
      return null;
    }
    return maxBkeyObject.getByteArrayBKeyRaw();
  }

  public Long getTrimmed() {
    return trimmed;
  }
}
