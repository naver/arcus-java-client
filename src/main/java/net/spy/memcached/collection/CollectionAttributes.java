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

import net.spy.memcached.util.BTreeUtil;

public class CollectionAttributes extends Attributes {

  public static final Long DEFAULT_MAXCOUNT = 4000L;
  public static final CollectionOverflowAction DEFAULT_OVERFLOWACTION =
      CollectionOverflowAction.tail_trim;

  private Long count;
  private Long maxCount;
  private CollectionOverflowAction overflowAction;
  private Boolean readable;

  private Long maxBkeyRange = null;
  private byte[] maxBkeyRangeByBytes = null;
  private Long minBkey = null;
  private byte[] minBkeyByBytes = null;
  private Long maxBkey = null;
  private byte[] maxBkeyByBytes = null;
  private Long trimmed = null;

  private String str;

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
    if (maxBkeyRange != null || maxBkeyRangeByBytes != null) {
      if (maxBkeyRange != null) {
        b.append(" maxbkeyrange=").append(String.valueOf(maxBkeyRange));
      } else {
        b.append(" maxbkeyrange=").append(
                BTreeUtil.toHex(maxBkeyRangeByBytes));
      }
    }

    str = (b.length() < 1) ? "" : b.substring(1);
    return str;
  }

  @Override
  public String toString() {
    return (str == null) ? stringify() : str;
  }

  public int getLength() {
    return (str == null) ? stringify().length() : str.length();
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
        type
                = CollectionType.find(value);
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
          maxBkeyRangeByBytes = BTreeUtil.hexStringToByteArrays(value.substring(2));
        } else {
          maxBkeyRange = Long.parseLong(value);
        }
      } else if ("minbkey".equals(name)) {
        if (!value.startsWith("-1")) {
          if (value.startsWith("0x")) {
            minBkeyByBytes = BTreeUtil.hexStringToByteArrays(value
                    .substring(2));
          } else {
            minBkey = Long.parseLong(value);
          }
        }
      } else if ("maxbkey".equals(name)) {
        if (!value.startsWith("-1")) {
          if (value.startsWith("0x")) {
            maxBkeyByBytes = BTreeUtil.hexStringToByteArrays(value
                    .substring(2));
          } else {
            maxBkey = Long.parseLong(value);
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

  public void setMaxCount(long maxCount) {
    this.str = null;
    this.maxCount = maxCount;
  }

  public void setOverflowAction(CollectionOverflowAction overflowAction) {
    this.str = null;
    this.overflowAction = overflowAction;
  }

  public void setReadable(Boolean readable) {
    this.str = null;
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
    return maxBkeyRange;
  }

  public void setMaxBkeyRange(Long maxBkeyRange) {
    this.str = null;
    this.maxBkeyRange = maxBkeyRange;
  }

  public byte[] getMaxBkeyRangeByBytes() {
    return maxBkeyRangeByBytes;
  }

  public void setMaxBkeyRangeByBytes(byte[] maxBkeyRangeByBytes) {
    this.maxBkeyRangeByBytes = maxBkeyRangeByBytes;
  }

  public Long getMinBkey() {
    return minBkey;
  }

  public byte[] getMinBkeyByBytes() {
    return minBkeyByBytes;
  }

  public Long getMaxBkey() {
    return maxBkey;
  }

  public byte[] getMaxBkeyByBytes() {
    return maxBkeyByBytes;
  }

  public Long getTrimmed() {
    return trimmed;
  }
}
