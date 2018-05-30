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
  protected int expTime;
  protected long maxCount;
  protected CollectionOverflowAction overflowAction;
  protected Boolean readable;
  protected boolean noreply;

  protected String str;

  public CollectionCreate() {
  }

  public CollectionCreate(int flags, Integer expTime, Long maxCount, CollectionOverflowAction overflowAction, Boolean readable, boolean noreply) {
    if (overflowAction != null) {
      if ((this instanceof SetCreate) &&
              !CollectionType.set.isAvailableOverflowAction(overflowAction)) {
        throw new IllegalArgumentException(overflowAction + " is unavailable overflow action in " + CollectionType.set + ".");
      } else if ((this instanceof ListCreate) &&
              !CollectionType.list.isAvailableOverflowAction(overflowAction)) {
        throw new IllegalArgumentException(overflowAction + " is unavailable overflow action in " + CollectionType.list + ".");
      } else if ((this instanceof MapCreate) &&
              !CollectionType.map.isAvailableOverflowAction(overflowAction)) {
        throw new IllegalArgumentException(overflowAction + " is unavailable overflow action in" + CollectionType.map + ".");
      } else if ((this instanceof BTreeCreate) &&
              !CollectionType.btree.isAvailableOverflowAction(overflowAction)) {
        throw new IllegalArgumentException(overflowAction + " is unavailable overflow action in " + CollectionType.btree + ".");
      }
    }
    this.flags = flags;
    this.expTime = (null == expTime) ? CollectionAttributes.DEFAULT_EXPIRETIME : expTime;
    this.maxCount = (null == maxCount) ? CollectionAttributes.DEFAULT_MAXCOUNT : maxCount;
    this.overflowAction = overflowAction;
    this.readable = readable;
    this.noreply = noreply;
  }

  public String stringify() {
    if (str != null) return str;

    StringBuilder b = new StringBuilder();

    b.append(flags);
    //TODO:collection insert with creation option
    b.append(' ').append(expTime);
    b.append(' ').append(maxCount);

    if (null != overflowAction) {
      b.append(' ').append(overflowAction);
    }

    if (null != readable && !readable) {
      b.append(' ').append("unreadable");
    }

    if (noreply) {
      b.append((b.length() <= 0) ? "" : " ").append("noreply");
    }

    str = b.toString();
    return str;
  }

  public String toString() {
    return (str != null) ? str : stringify();
  }

  public abstract String getCommand();
}
