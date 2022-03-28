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
import net.spy.memcached.transcoders.Transcoder;
import net.spy.memcached.util.BTreeUtil;

public abstract class CollectionBulkInsert<T> extends CollectionObject {

  public static final String PIPE = "pipe";

  protected List<String> keyList;
  protected T value;
  protected CachedData cachedData;
  protected Transcoder<T> tc;
  protected int itemCount;

  protected CollectionAttributes attribute;
  protected MemcachedNode node;

  protected int nextOpIndex = 0;

  /**
   * set next index of operation
   * that will be processed after when operation moved by switchover
   */
  public void setNextOpIndex(int i) {
    this.nextOpIndex = i;
  }

  public int getNextOpIndex() {
    return nextOpIndex;
  }

  public MemcachedNode getMemcachedNode() {
    return node;
  }

  public abstract ByteBuffer getAsciiCommand();

  public abstract ByteBuffer getBinaryCommand();

  /**
   *
   */
  public static class BTreeBulkInsert<T> extends CollectionBulkInsert<T> {

    private static final String COMMAND = "bop insert";

    private final String bkey;
    private final String eflag;

    public BTreeBulkInsert(MemcachedNode node, List<String> keyList, String bkey,
                           byte[] eflag, T value, CollectionAttributes attr, Transcoder<T> tc) {
      if (attr != null) { /* item creation option */
        CollectionCreate.checkOverflowAction(CollectionType.btree, attr.getOverflowAction());
      }
      this.node = node;
      this.keyList = keyList;
      this.bkey = bkey;
      this.eflag = BTreeUtil.toHex(eflag);
      this.value = value;
      this.attribute = attr;
      this.tc = tc;
      this.itemCount = keyList.size();
      this.cachedData = tc.encode(value);
    }

    public BTreeBulkInsert(MemcachedNode node, List<String> keyList, Long bkey,
                           byte[] eflag, T value, CollectionAttributes attr, Transcoder<T> tc) {
      this(node, keyList, String.valueOf(bkey), eflag, value, attr, tc);
    }

    public BTreeBulkInsert(MemcachedNode node, List<String> keyList, byte[] bkey,
                           byte[] eflag, T value, CollectionAttributes attr, Transcoder<T> tc) {
      this(node, keyList, BTreeUtil.toHex(bkey), eflag, value, attr, tc);
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
      String createOption = attribute != null ?
          CollectionCreate.makeCreateClause(attribute, cachedData.getFlags()) : "";
      for (int i = this.nextOpIndex; i < kSize; i++) {
        String key = keyList.get(i);
        setArguments(bb, COMMAND, key, bkey, (eflag != null) ? eflag : "", value.length,
                     createOption,  (i < kSize - 1) ? PIPE : "");
        bb.put(value);
        bb.put(CRLF);
      }

      // flip the buffer
      ((Buffer) bb).flip();

      return bb;
    }

    public ByteBuffer getBinaryCommand() {
      throw new RuntimeException("not supported in binary protocol yet.");
    }
  }

  public static class MapBulkInsert<T> extends CollectionBulkInsert<T> {

    private static final String COMMAND = "mop insert";
    private final String mkey;

    public MapBulkInsert(MemcachedNode node, List<String> keyList, String mkey,
                         T value, CollectionAttributes attr, Transcoder<T> tc) {
      if (attr != null) { /* item creation option */
        CollectionCreate.checkOverflowAction(CollectionType.map, attr.getOverflowAction());
      }
      this.node = node;
      this.keyList = keyList;
      this.mkey = mkey;
      this.value = value;
      this.attribute = attr;
      this.tc = tc;
      this.itemCount = keyList.size();
      this.cachedData = tc.encode(value);
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
      String createOption = attribute != null ?
          CollectionCreate.makeCreateClause(attribute, cachedData.getFlags()) : "";
      for (int i = this.nextOpIndex; i < kSize; i++) {
        String key = keyList.get(i);
        setArguments(bb, COMMAND, key, mkey, value.length,
                     createOption, (i < kSize - 1) ? PIPE : "");
        bb.put(value);
        bb.put(CRLF);
      }

      // flip the buffer
      ((Buffer) bb).flip();

      return bb;
    }

    public ByteBuffer getBinaryCommand() {
      throw new RuntimeException("not supported in binary protocol yet.");
    }
  }

  public static class SetBulkInsert<T> extends CollectionBulkInsert<T> {

    private static final String COMMAND = "sop insert";

    public SetBulkInsert(MemcachedNode node, List<String> keyList, T value,
                         CollectionAttributes attr, Transcoder<T> tc) {
      if (attr != null) { /* item creation option */
        CollectionCreate.checkOverflowAction(CollectionType.set, attr.getOverflowAction());
      }
      this.node = node;
      this.keyList = keyList;
      this.value = value;
      this.attribute = attr;
      this.tc = tc;
      this.itemCount = keyList.size();
      this.cachedData = tc.encode(value);
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
      String createOption = attribute != null ?
          CollectionCreate.makeCreateClause(attribute, cachedData.getFlags()) : "";
      for (int i = this.nextOpIndex; i < kSize; i++) {
        String key = keyList.get(i);
        setArguments(bb, COMMAND, key, value.length,
                     createOption, (i < kSize - 1) ? PIPE : "");
        bb.put(value);
        bb.put(CRLF);
      }
      // flip the buffer
      ((Buffer) bb).flip();

      return bb;
    }

    public ByteBuffer getBinaryCommand() {
      throw new RuntimeException("not supported in binary protocol yet.");
    }
  }

  public static class ListBulkInsert<T> extends CollectionBulkInsert<T> {

    private static final String COMMAND = "lop insert";
    private int index;

    public ListBulkInsert(MemcachedNode node, List<String> keyList, int index,
                          T value, CollectionAttributes attr, Transcoder<T> tc) {
      if (attr != null) { /* item creation option */
        CollectionCreate.checkOverflowAction(CollectionType.list, attr.getOverflowAction());
      }
      this.node = node;
      this.keyList = keyList;
      this.index = index;
      this.value = value;
      this.attribute = attr;
      this.tc = tc;
      this.itemCount = keyList.size();
      this.cachedData = tc.encode(value);
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
      String createOption = attribute != null ?
          CollectionCreate.makeCreateClause(attribute, cachedData.getFlags()) : "";
      for (int i = this.nextOpIndex; i < kSize; i++) {
        String key = this.keyList.get(i);
        setArguments(bb, COMMAND, key, index, value.length,
                     createOption, (i < kSize - 1) ? PIPE : "");
        bb.put(value);
        bb.put(CRLF);
      }

      // flip the buffer
      ((Buffer) bb).flip();

      return bb;
    }

    public ByteBuffer getBinaryCommand() {
      throw new RuntimeException("not supported in binary protocol yet.");
    }
  }

  public List<String> getKeyList() {
    return this.keyList;
  }

  public int getItemCount() {
    return this.itemCount;
  }
}
