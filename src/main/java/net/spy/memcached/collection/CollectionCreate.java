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

public abstract class CollectionCreate {
  protected int flags;
  protected CreateAttributes attributes;
  protected boolean noreply;

  protected String str;

  protected CollectionCreate(CollectionType type, int flags,
                             CreateAttributes attributes, boolean noreply) {
    checkOverflowAction(type, attributes.getOverflowAction());
    this.flags = flags;
    this.attributes = attributes;
    this.noreply = noreply;
  }

  public String stringify() {
    if (str != null) {
      return str;
    }

    StringBuilder b = new StringBuilder();

    b.append(flags);
    b.append(' ').append(attributes.getExpireTime());
    b.append(' ').append(attributes.getMaxCount());

    if (null != attributes.getOverflowAction()) {
      b.append(' ').append(attributes.getOverflowAction());
    }

    if (!attributes.getReadable()) {
      b.append(' ').append("unreadable");
    }

    if (noreply) {
      b.append((b.length() <= 0) ? "" : " ").append("noreply");
    }

    str = b.toString();
    return str;
  }

  public static String makeCreateClause(CreateAttributes attributes, int flags) {
    if (attributes == null) {
      return null;
    }
    StringBuilder b = new StringBuilder();
    b.append("create ").append(flags)
        .append(" ").append(attributes.getExpireTime())
        .append(" ").append(attributes.getMaxCount());
    if (attributes.getOverflowAction() != null) {
      b.append(" ").append(attributes.getOverflowAction());
    }
    if (!attributes.getReadable()) {
      b.append(" ").append("unreadable");
    }
    return b.toString();
  }

  public static void checkOverflowAction(CollectionType type, CollectionOverflowAction action) {
    if (action != null && !type.isAvailableOverflowAction(action)) {
      throw new IllegalArgumentException(
          action + " is unavailable overflow action in " + type + ".");
    }
  }

  public String toString() {
    return (str != null) ? str : stringify();
  }

  public abstract String getCommand();
}
