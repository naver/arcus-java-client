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

import java.nio.ByteBuffer;
import java.util.List;

import net.spy.memcached.CachedData;
import net.spy.memcached.KeyUtil;
import net.spy.memcached.transcoders.Transcoder;
import net.spy.memcached.util.BTreeUtil;

public abstract class CollectionBulkStore<T> extends CollectionObject {

  public static final String PIPE = "pipe";
  public static final int MAX_PIPED_ITEM_COUNT = 500;

  protected List<String> keyList;
  protected T value;
  protected CachedData cachedData;
  protected boolean createKeyIfNotExists;
  protected Transcoder<T> tc;
  protected int itemCount;

  protected CollectionAttributes attribute;

  protected int nextOpIndex = 0;

  /**
   * set next index of operation
   * that will be processed after when operation moved by switchover
   */
  public void setNextOpIndex(int i) {
    this.nextOpIndex = i;
  }

  /* ENABLE_MIGRATION if */
  public int getNextOpIndex() {
    return this.nextOpIndex;
  }
  /* ENABLE_MIGRATION end */

  public abstract ByteBuffer getAsciiCommand();

  public abstract ByteBuffer getBinaryCommand();

  /* ENABLE_MIGRATION if */
  public abstract CollectionBulkStore<T> makeSingleStore(List<String> singleKeyList);
  /* ENABLE_MIGRATION end */

  /**
   *
   */
  public static class BTreeBulkStore<T> extends CollectionBulkStore<T> {

    private static final String COMMAND = "bop insert";

    private final String bkey;
    private final String eflag;

    public BTreeBulkStore(List<String> keyList, long bkey, byte[] eflag,
                          T value, CollectionAttributes attr, Transcoder<T> tc) {
      if (attr != null) {
        CollectionOverflowAction overflowAction = attr.getOverflowAction();
        if (overflowAction != null && !CollectionType.btree.isAvailableOverflowAction(overflowAction))
          throw new IllegalArgumentException(overflowAction + " is unavailable overflow action in " + CollectionType.btree + ".");
      }
      this.keyList = keyList;
      this.bkey = String.valueOf(bkey);
      this.eflag = BTreeUtil.toHex(eflag);
      this.value = value;
      this.attribute = attr;
      this.tc = tc;
      this.itemCount = keyList.size();
      this.createKeyIfNotExists = (attr != null);
      this.cachedData = tc.encode(value);
    }

    public BTreeBulkStore(List<String> keyList, byte[] bkey,
                          byte[] eflag, T value, CollectionAttributes attr,
                          Transcoder<T> tc) {
      if (attr != null) {
        CollectionOverflowAction overflowAction = attr.getOverflowAction();
        if (overflowAction != null && !CollectionType.btree.isAvailableOverflowAction(overflowAction))
          throw new IllegalArgumentException(overflowAction + " is unavailable overflow action in " + CollectionType.btree + ".");
      }
      this.keyList = keyList;
      this.bkey = BTreeUtil.toHex(bkey);
      this.eflag = BTreeUtil.toHex(eflag);
      this.value = value;
      this.attribute = attr;
      this.tc = tc;
      this.itemCount = keyList.size();
      this.createKeyIfNotExists = (attr != null);
      this.cachedData = tc.encode(value);
    }

    /* ENABLE_MIGRAETION if */
    public BTreeBulkStore(List<String> keyList, String bkey, String eflag,
                          T value, CollectionAttributes attr, Transcoder<T> tc) {
      if (attr != null) {
        CollectionOverflowAction overflowAction = attr.getOverflowAction();
        if (overflowAction != null && !CollectionType.btree.isAvailableOverflowAction(overflowAction))
          throw new IllegalArgumentException(overflowAction + " is unavailable overflow action in " + CollectionType.btree + ".");
      }
      this.keyList = keyList;
      this.bkey = bkey;
      this.eflag = eflag;
      this.value = value;
      this.attribute = attr;
      this.tc = tc;
      this.itemCount = keyList.size();
      this.createKeyIfNotExists = (attr != null);
      this.cachedData = tc.encode(value);
    }

    @Override
    public CollectionBulkStore<T> makeSingleStore(List<String> singleKeyList) {
      return new BTreeBulkStore<T>(singleKeyList, this.bkey, this.eflag, this.value, this.attribute, this.tc);
    }
    /* ENABLE_MIGRATION end */

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
      for (int i = this.nextOpIndex; i < kSize; i++) {
        String key = keyList.get(i);
        byte[] value = cachedData.getData();

        setArguments(
                bb,
                COMMAND,
                key,
                bkey,
                (eflag != null) ? eflag : "",
                value.length,
                (createKeyIfNotExists) ? "create" : "",
                (createKeyIfNotExists) ? cachedData.getFlags() : "",
                (createKeyIfNotExists) ? (attribute != null && attribute
                        .getExpireTime() != null) ? attribute
                        .getExpireTime()
                        : CollectionAttributes.DEFAULT_EXPIRETIME : "",
                (createKeyIfNotExists) ? (attribute != null && attribute
                        .getMaxCount() != null) ? attribute
                        .getMaxCount()
                        : CollectionAttributes.DEFAULT_MAXCOUNT : "",
                (createKeyIfNotExists) ? (attribute != null && attribute
                        .getOverflowAction() != null) ? attribute
                        .getOverflowAction()
                        : "" : "",
                (createKeyIfNotExists) ? (attribute != null && attribute
                        .getReadable() != null && !attribute.getReadable()) ?
                        "unreadable"
                        : "" : "",
                (i < kSize - 1) ? PIPE : "");
        bb.put(value);
        bb.put(CRLF);
      }

      // flip the buffer
      bb.flip();

      return bb;
    }

    public ByteBuffer getBinaryCommand() {
      throw new RuntimeException("not supported in binary protocol yet.");
    }
  }

  public static class MapBulkStore<T> extends CollectionBulkStore<T> {

    private static final String COMMAND = "mop insert";
    private final String mkey;

    public MapBulkStore(List<String> keyList, String mkey, T value,
                        CollectionAttributes attr, Transcoder<T> tc) {
      if (attr != null) {
        CollectionOverflowAction overflowAction = attr.getOverflowAction();
        if (overflowAction != null && !CollectionType.map.isAvailableOverflowAction(overflowAction))
          throw new IllegalArgumentException(overflowAction + " is unavailable overflow action in " + CollectionType.map + ".");
      }
      this.keyList = keyList;
      this.mkey = mkey;
      this.value = value;
      this.attribute = attr;
      this.tc = tc;
      this.itemCount = keyList.size();
      this.createKeyIfNotExists = (attr != null);
      this.cachedData = tc.encode(value);
    }

    /* ENABLE_MIGRATION if */
    @Override
    public CollectionBulkStore<T> makeSingleStore(List<String> singleKeyList) {
      return new MapBulkStore<T>(singleKeyList, this.mkey, this.value, this.attribute, this.tc);
    }
    /* ENABLE_MIGRATION end */

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
      for (int i = this.nextOpIndex; i < kSize; i++) {
        String key = keyList.get(i);
        byte[] value = cachedData.getData();

        setArguments(
                bb,
                COMMAND,
                key,
                mkey,
                value.length,
                (createKeyIfNotExists) ? "create" : "",
                (createKeyIfNotExists) ? cachedData.getFlags() : "",
                (createKeyIfNotExists) ? (attribute != null && attribute
                        .getExpireTime() != null) ? attribute
                        .getExpireTime()
                        : CollectionAttributes.DEFAULT_EXPIRETIME : "",
                (createKeyIfNotExists) ? (attribute != null && attribute
                        .getMaxCount() != null) ? attribute
                        .getMaxCount()
                        : CollectionAttributes.DEFAULT_MAXCOUNT : "",
                (createKeyIfNotExists) ? (attribute != null && attribute
                        .getOverflowAction() != null) ? attribute
                        .getOverflowAction()
                        : "" : "",
                (createKeyIfNotExists) ? (attribute != null && attribute
                        .getReadable() != null && !attribute.getReadable()) ?
                        "unreadable"
                        : "" : "",
                (i < kSize - 1) ? PIPE : "");
        bb.put(value);
        bb.put(CRLF);
      }

      // flip the buffer
      bb.flip();

      return bb;
    }

    public ByteBuffer getBinaryCommand() {
      throw new RuntimeException("not supported in binary protocol yet.");
    }
  }

  public static class SetBulkStore<T> extends CollectionBulkStore<T> {

    private static final String COMMAND = "sop insert";

    public SetBulkStore(List<String> keyList, T value,
                        CollectionAttributes attr, Transcoder<T> tc) {
      if (attr != null) {
        CollectionOverflowAction overflowAction = attr.getOverflowAction();
        if (overflowAction != null && !CollectionType.set.isAvailableOverflowAction(overflowAction))
          throw new IllegalArgumentException(overflowAction + " is unavailable overflow action in " + CollectionType.set + ".");
      }
      this.keyList = keyList;
      this.value = value;
      this.attribute = attr;
      this.tc = tc;
      this.itemCount = keyList.size();
      this.createKeyIfNotExists = (attr != null);
      this.cachedData = tc.encode(value);
    }

    /* ENABLE_MIGRATION if */
    @Override
    public CollectionBulkStore<T> makeSingleStore(List<String> singleKeyList) {
      return new SetBulkStore<T>(singleKeyList, this.value, this.attribute, this.tc);
    }
    /* ENABLE_MIGRATION end */

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
      for (int i = this.nextOpIndex; i < kSize; i++) {
        String key = keyList.get(i);
        byte[] value = cachedData.getData();

        setArguments(
                bb,
                COMMAND,
                key,
                value.length,
                (createKeyIfNotExists) ? "create" : "",
                (createKeyIfNotExists) ? cachedData.getFlags() : "",
                (createKeyIfNotExists) ? (attribute != null && attribute
                        .getExpireTime() != null) ? attribute
                        .getExpireTime()
                        : CollectionAttributes.DEFAULT_EXPIRETIME : "",
                (createKeyIfNotExists) ? (attribute != null && attribute
                        .getMaxCount() != null) ? attribute
                        .getMaxCount()
                        : CollectionAttributes.DEFAULT_MAXCOUNT : "",
                (createKeyIfNotExists) ? (attribute != null && attribute
                        .getOverflowAction() != null) ? attribute
                        .getOverflowAction()
                        : "" : "",
                (createKeyIfNotExists) ? (attribute != null && attribute
                        .getReadable() != null && !attribute.getReadable()) ?
                        "unreadable"
                        : "" : "",
                (i < kSize - 1) ? PIPE : "");
        bb.put(value);
        bb.put(CRLF);
      }
      // flip the buffer
      bb.flip();

      return bb;
    }

    public ByteBuffer getBinaryCommand() {
      throw new RuntimeException("not supported in binary protocol yet.");
    }
  }

  public static class ListBulkStore<T> extends CollectionBulkStore<T> {

    private static final String COMMAND = "lop insert";
    private int index;

    public ListBulkStore(List<String> keyList, int index, T value,
                         CollectionAttributes attr, Transcoder<T> tc) {
      if (attr != null) {
        CollectionOverflowAction overflowAction = attr.getOverflowAction();
        if (overflowAction != null && !CollectionType.list.isAvailableOverflowAction(overflowAction))
          throw new IllegalArgumentException(overflowAction + " is unavailable overflow action in " + CollectionType.list + ".");
      }
      this.keyList = keyList;
      this.index = index;
      this.value = value;
      this.attribute = attr;
      this.tc = tc;
      this.itemCount = keyList.size();
      this.createKeyIfNotExists = (attr != null);
      this.cachedData = tc.encode(value);
    }

    /* ENABLE_MIGRATION if */
    @Override
    public CollectionBulkStore<T> makeSingleStore(List<String> singleKeyList) {
      return new ListBulkStore<T>(singleKeyList, this.index, this.value, this.attribute, this.tc);
    }
    /* ENABLE_MIGRATION end */

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
      for (int i = this.nextOpIndex; i < kSize; i++) {
        String key = this.keyList.get(i);
        byte[] value = cachedData.getData();

        setArguments(
                bb,
                COMMAND,
                key,
                index,
                value.length,
                (createKeyIfNotExists) ? "create" : "",
                (createKeyIfNotExists) ? cachedData.getFlags() : "",
                (createKeyIfNotExists) ? (attribute != null && attribute
                        .getExpireTime() != null) ? attribute
                        .getExpireTime()
                        : CollectionAttributes.DEFAULT_EXPIRETIME : "",
                (createKeyIfNotExists) ? (attribute != null && attribute
                        .getMaxCount() != null) ? attribute
                        .getMaxCount()
                        : CollectionAttributes.DEFAULT_MAXCOUNT : "",
                (createKeyIfNotExists) ? (attribute != null && attribute
                        .getOverflowAction() != null) ? attribute
                        .getOverflowAction()
                        : "" : "",
                (createKeyIfNotExists) ? (attribute != null && attribute
                        .getReadable() != null && !attribute.getReadable()) ?
                        "unreadable"
                        : "" : "",
                (i < kSize - 1) ? PIPE : "");
        bb.put(value);
        bb.put(CRLF);
      }

      // flip the buffer
      bb.flip();

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
