/*
 * arcus-java-client : Arcus Java client
 * Copyright 2010-2014 NAVER Corp.
 * Copyright 2014-2021 JaM2in Co., Ltd.
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

import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.util.List;

import net.spy.memcached.CachedData;
import net.spy.memcached.KeyUtil;
import net.spy.memcached.MemcachedNode;

public abstract class CollectionBulkInsert<T> extends CollectionPipe {

  protected final MemcachedNode node;
  protected final List<String> keyList;
  protected final CachedData cachedData;
  protected final CreateAttributes attributes;

  protected CollectionBulkInsert(MemcachedNode node, List<String> keyList,
                                 CachedData cachedData, CreateAttributes attributes) {
    super(keyList.size());
    this.node = node;
    this.keyList = keyList;
    this.cachedData = cachedData;
    this.attributes = attributes;
  }

  public String getKey(int index) {
    return this.keyList.get(index);
  }

  public List<String> getKeyList() {
    return this.keyList;
  }

  public MemcachedNode getMemcachedNode() {
    return node;
  }

  public abstract CollectionBulkInsert<T> clone(MemcachedNode node,
                                                List<String> keyList);

  /**
   *
   */
  public static class BTreeBulkInsert<T> extends CollectionBulkInsert<T> {

    private static final String COMMAND = "bop insert";

    private final String bkey;
    private final String eflag;

    public BTreeBulkInsert(MemcachedNode node, List<String> keyList, String bkey,
                           String eflag, CachedData cachedData,
                           CreateAttributes attributes) {
      super(node, keyList, cachedData, attributes);
      if (attributes != null) { /* item creation option */
        CollectionType.btree.checkOverflowAction(attributes.getOverflowAction());
      }
      this.bkey = bkey;
      this.eflag = eflag;
    }

    public ByteBuffer getAsciiCommand() {
      int capacity = 0;

      // estimate the buffer capacity
      int eachExtraSize = bkey.length()
          + ((eflag != null) ? eflag.length() : 0)
          + cachedData.getData().length + 128;
      for (String eachKey : keyList) {
        capacity += KeyUtil.getKeyBytes(eachKey).length;
      }
      capacity += eachExtraSize * keyList.size();

      // allocate the buffer
      ByteBuffer bb = ByteBuffer.allocate(capacity);

      // create ascii operation string
      int kSize = this.keyList.size();
      byte[] value = cachedData.getData();
      String createClause = attributes != null ?
          attributes.toCreateClause(cachedData.getFlags()) : "";
      for (int i = this.nextOpIndex; i < kSize; i++) {
        String key = keyList.get(i);
        setArguments(bb, COMMAND, key, bkey, (eflag != null) ? eflag : "", value.length,
            createClause, (i < kSize - 1) ? PIPE : "");
        bb.put(value);
        bb.put(CRLF);
      }

      // flip the buffer
      ((Buffer) bb).flip();

      return bb;
    }

    @Override
    public CollectionBulkInsert<T> clone(MemcachedNode node,
                                         List<String> keyList) {
      return new BTreeBulkInsert<>(node, keyList, bkey, eflag, cachedData, attributes);
    }
  }

  public static class MapBulkInsert<T> extends CollectionBulkInsert<T> {

    private static final String COMMAND = "mop insert";
    private final String mkey;

    public MapBulkInsert(MemcachedNode node, List<String> keyList, String mkey,
                         CachedData cachedData, CreateAttributes attributes) {
      super(node, keyList, cachedData, attributes);
      if (attributes != null) { /* item creation option */
        CollectionType.map.checkOverflowAction(attributes.getOverflowAction());
      }
      this.mkey = mkey;
    }

    public ByteBuffer getAsciiCommand() {
      int capacity = 0;

      // estimate the buffer capacity
      int eachExtraSize = KeyUtil.getKeyBytes(mkey).length
          + cachedData.getData().length + 128;
      for (String eachKey : keyList) {
        capacity += KeyUtil.getKeyBytes(eachKey).length;
      }
      capacity += eachExtraSize * keyList.size();

      // allocate the buffer
      ByteBuffer bb = ByteBuffer.allocate(capacity);

      // create ascii operation string
      int kSize = this.keyList.size();
      byte[] value = cachedData.getData();
      String createClause = attributes != null ?
          attributes.toCreateClause(cachedData.getFlags()) : "";
      for (int i = this.nextOpIndex; i < kSize; i++) {
        String key = keyList.get(i);
        setArguments(bb, COMMAND, key, mkey, value.length,
            createClause, (i < kSize - 1) ? PIPE : "");
        bb.put(value);
        bb.put(CRLF);
      }

      // flip the buffer
      ((Buffer) bb).flip();

      return bb;
    }

    @Override
    public CollectionBulkInsert<T> clone(MemcachedNode node,
                                         List<String> keyList) {
      return new MapBulkInsert<>(node, keyList, mkey, cachedData, attributes);
    }
  }

  public static class SetBulkInsert<T> extends CollectionBulkInsert<T> {

    private static final String COMMAND = "sop insert";

    public SetBulkInsert(MemcachedNode node, List<String> keyList,
                         CachedData cachedData, CreateAttributes attributes) {
      super(node, keyList, cachedData, attributes);
      if (attributes != null) { /* item creation option */
        CollectionType.set.checkOverflowAction(attributes.getOverflowAction());
      }
    }

    public ByteBuffer getAsciiCommand() {
      int capacity = 0;

      // estimate the buffer capacity
      int eachExtraSize = cachedData.getData().length + 128;
      for (String eachKey : keyList) {
        capacity += KeyUtil.getKeyBytes(eachKey).length;
      }
      capacity += eachExtraSize * keyList.size();

      // allocate the buffer
      ByteBuffer bb = ByteBuffer.allocate(capacity);

      // create ascii operation string
      int kSize = this.keyList.size();
      byte[] value = cachedData.getData();
      String createClause = attributes != null ?
          attributes.toCreateClause(cachedData.getFlags()) : "";
      for (int i = this.nextOpIndex; i < kSize; i++) {
        String key = keyList.get(i);
        setArguments(bb, COMMAND, key, value.length,
            createClause, (i < kSize - 1) ? PIPE : "");
        bb.put(value);
        bb.put(CRLF);
      }
      // flip the buffer
      ((Buffer) bb).flip();

      return bb;
    }

    @Override
    public CollectionBulkInsert<T> clone(MemcachedNode node,
                                         List<String> keyList) {
      return new SetBulkInsert<>(node, keyList, cachedData, attributes);
    }
  }

  public static class ListBulkInsert<T> extends CollectionBulkInsert<T> {

    private static final String COMMAND = "lop insert";
    private final int index;

    public ListBulkInsert(MemcachedNode node, List<String> keyList, int index,
                          CachedData cachedData, CreateAttributes attributes) {
      super(node, keyList, cachedData, attributes);
      if (attributes != null) { /* item creation option */
        CollectionType.list.checkOverflowAction(attributes.getOverflowAction());
      }
      this.index = index;
    }

    public ByteBuffer getAsciiCommand() {
      int capacity = 0;

      // estimate the buffer capacity
      int eachExtraSize = String.valueOf(index).length()
          + cachedData.getData().length + 128;
      for (String eachKey : keyList) {
        capacity += KeyUtil.getKeyBytes(eachKey).length;
      }
      capacity += eachExtraSize * keyList.size();

      // allocate the buffer
      ByteBuffer bb = ByteBuffer.allocate(capacity);

      // create ascii operation string
      int kSize = keyList.size();
      byte[] value = cachedData.getData();
      String createClause = attributes != null ?
          attributes.toCreateClause(cachedData.getFlags()) : "";
      for (int i = this.nextOpIndex; i < kSize; i++) {
        String key = this.keyList.get(i);
        setArguments(bb, COMMAND, key, index, value.length,
            createClause, (i < kSize - 1) ? PIPE : "");
        bb.put(value);
        bb.put(CRLF);
      }

      // flip the buffer
      ((Buffer) bb).flip();

      return bb;
    }

    @Override
    public CollectionBulkInsert<T> clone(MemcachedNode node,
                                         List<String> keyList) {
      return new ListBulkInsert<>(node, keyList, index, cachedData, attributes);
    }
  }
}
