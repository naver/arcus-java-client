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

import net.spy.memcached.compat.SpyObject;

public class Attributes extends SpyObject {
  public static final Integer DEFAULT_FLAGS = 0;
  public static final Integer DEFAULT_EXPIRETIME = 0;

  protected Integer flags;
  protected Integer expireTime;
  protected CollectionType type;

  private String str;

  public Attributes() {
  }

  public Attributes(Integer expireTime) {
    this.expireTime = expireTime;
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
        type = CollectionType.find(value);
      }
    } catch (Exception e) {
      getLogger().info(e, e);
      assert false : "Unexpected value.";
    }
  }

  public void setExpireTime(Integer expireTime) {
    this.expireTime = expireTime;
  }

  public Integer getFlags() {
    return flags;
  }

  public Integer getExpireTime() {
    return expireTime;
  }

  public CollectionType getType() {
    return type;
  }
}
