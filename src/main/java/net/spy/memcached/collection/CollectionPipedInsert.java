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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import net.spy.memcached.CachedData;
import net.spy.memcached.KeyUtil;
import net.spy.memcached.transcoders.Transcoder;

public abstract class CollectionPipedInsert<T> extends CollectionPipe {

  public static final int MAX_PIPED_ITEM_COUNT = 500;

  protected final String key;
  protected final CollectionAttributes attribute;
  protected final Transcoder<T> tc;

  protected CollectionPipedInsert(String key, CollectionAttributes attribute,
                                  Transcoder<T> tc, int itemCount) {
    super(itemCount);
    this.key = key;
    this.attribute = attribute;
    this.tc = tc;
  }

  /**
   *
   */
  public static class ListPipedInsert<T> extends CollectionPipedInsert<T> {

    private static final String COMMAND = "lop insert";
    private final Collection<T> list;
    private final int index;

    public ListPipedInsert(String key, int index, Collection<T> list,
                           CollectionAttributes attr, Transcoder<T> tc) {
      super(key, attr, tc, list.size());
      if (attr != null) { /* item creation option */
        CollectionCreate.checkOverflowAction(CollectionType.list, attr.getOverflowAction());
      }
      this.index = index;
      this.list = list;
    }

    public ByteBuffer getAsciiCommand() {
      int capacity = 0;

      // encode values
      List<byte[]> encodedList = new ArrayList<>(list.size());
      CachedData cd = null;
      for (T each : list) {
        cd = tc.encode(each);
        encodedList.add(cd.getData());
      }

      // estimate the buffer capacity
      for (byte[] each : encodedList) {
        capacity += KeyUtil.getKeyBytes(key).length;
        capacity += each.length;
        capacity += 128;
      }

      // allocate the buffer
      ByteBuffer bb = ByteBuffer.allocate(capacity);

      // create ascii operation string
      int eSize = encodedList.size();
      String createOption = attribute != null ?
          CollectionCreate.makeCreateClause(attribute, cd.getFlags()) : "";
      for (int i = this.nextOpIndex; i < eSize; i++) {
        byte[] each = encodedList.get(i);
        int eIndex = index;
        if (index >= 0) {
          eIndex += i;
        }
        setArguments(bb, COMMAND, key, eIndex, each.length,
                     createOption, (i < eSize - 1) ? PIPE : "");
        bb.put(each);
        bb.put(CRLF);
      }

      // flip the buffer
      bb.flip();

      return bb;
    }
  }

  /**
   *
   */
  public static class SetPipedInsert<T> extends CollectionPipedInsert<T> {

    private static final String COMMAND = "sop insert";
    private final Collection<T> set;

    public SetPipedInsert(String key, Collection<T> set,
                          CollectionAttributes attr, Transcoder<T> tc) {
      super(key, attr, tc, set.size());
      if (attr != null) { /* item creation option */
        CollectionCreate.checkOverflowAction(CollectionType.set, attr.getOverflowAction());
      }
      this.set = set;
    }

    public ByteBuffer getAsciiCommand() {
      int capacity = 0;

      // encode values
      List<byte[]> encodedList = new ArrayList<>(set.size());
      CachedData cd = null;
      for (T each : set) {
        cd = tc.encode(each);
        encodedList.add(cd.getData());
      }

      // estimate the buffer capacity
      for (byte[] each : encodedList) {
        capacity += KeyUtil.getKeyBytes(key).length;
        capacity += each.length;
        capacity += 128;
      }

      // allocate the buffer
      ByteBuffer bb = ByteBuffer.allocate(capacity);

      // create ascii operation string
      int eSize = encodedList.size();
      String createOption = attribute != null ?
          CollectionCreate.makeCreateClause(attribute, cd.getFlags()) : "";
      for (int i = this.nextOpIndex; i < eSize; i++) {
        byte[] each = encodedList.get(i);
        setArguments(bb, COMMAND, key, each.length,
            createOption, (i < eSize - 1) ? PIPE : "");
        bb.put(each);
        bb.put(CRLF);
      }

      // flip the buffer
      bb.flip();

      return bb;
    }
  }

  /**
   *
   */
  public static class BTreePipedInsert<T> extends CollectionPipedInsert<T> {

    private static final String COMMAND = "bop insert";
    private final Map<Long, T> map;

    public BTreePipedInsert(String key, Map<Long, T> map,
                            CollectionAttributes attr, Transcoder<T> tc) {
      super(key, attr, tc, map.size());
      if (attr != null) { /* item creation option */
        CollectionCreate.checkOverflowAction(CollectionType.btree, attr.getOverflowAction());
      }
      this.map = map;
    }

    public ByteBuffer getAsciiCommand() {
      int capacity = 0;

      // encode parameters
      List<byte[]> encodedList = new ArrayList<>(map.size());
      CachedData cd = null;
      for (T each : map.values()) {
        cd = tc.encode(each);
        encodedList.add(cd.getData());
      }

      // estimate the buffer capacity
      int i = 0;
      for (Long eachBkey : map.keySet()) {
        capacity += KeyUtil.getKeyBytes(key).length;
        capacity += KeyUtil.getKeyBytes(String.valueOf(eachBkey)).length;
        capacity += encodedList.get(i++).length;
        capacity += 128;
      }

      // allocate the buffer
      ByteBuffer bb = ByteBuffer.allocate(capacity);

      // create ascii operation string
      int keySize = map.keySet().size();
      List<Long> keyList = new ArrayList<>(map.keySet());
      String createOption = attribute != null ?
          CollectionCreate.makeCreateClause(attribute, cd.getFlags()) : "";
      for (i = this.nextOpIndex; i < keySize; i++) {
        Long bkey = keyList.get(i);
        byte[] value = encodedList.get(i);
        setArguments(bb, COMMAND, key, bkey, value.length,
            createOption, (i < keySize - 1) ? PIPE : "");
        bb.put(value);
        bb.put(CRLF);
      }

      // flip the buffer
      bb.flip();

      return bb;
    }
  }

  /**
   *
   */
  public static class ByteArraysBTreePipedInsert<T> extends
          CollectionPipedInsert<T> {

    private static final String COMMAND = "bop insert";
    private final List<Element<T>> elements;

    public ByteArraysBTreePipedInsert(String key, List<Element<T>> elements,
                                      CollectionAttributes attr, Transcoder<T> tc) {
      super(key, attr, tc, elements.size());
      if (attr != null) { /* item creation option */
        CollectionCreate.checkOverflowAction(CollectionType.btree, attr.getOverflowAction());
      }
      this.elements = elements;
    }

    public ByteBuffer getAsciiCommand() {
      int capacity = 0;

      // encode parameters
      List<byte[]> encodedList = new ArrayList<>(elements.size());
      CachedData cd = null;
      for (Element<T> each : elements) {
        cd = tc.encode(each.getValue());
        encodedList.add(cd.getData());
      }

      // estimate the buffer capacity
      int i = 0;
      for (Element<T> each : elements) {
        capacity += KeyUtil.getKeyBytes(key).length;
        capacity += KeyUtil.getKeyBytes(each.getStringBkey()).length;
        capacity += KeyUtil.getKeyBytes(each.getStringEFlag()).length;
        capacity += encodedList.get(i++).length;
        capacity += 128;
      }

      // allocate the buffer
      ByteBuffer bb = ByteBuffer.allocate(capacity);

      // create ascii operation string
      int eSize = elements.size();
      String createOption = attribute != null ?
          CollectionCreate.makeCreateClause(attribute, cd.getFlags()) : "";
      for (i = this.nextOpIndex; i < eSize; i++) {
        Element<T> element = elements.get(i);
        byte[] value = encodedList.get(i);
        setArguments(bb, COMMAND, key,
                     element.getStringBkey(), element.getStringEFlag(), value.length,
                     createOption, (i < eSize - 1) ? PIPE : "");
        bb.put(value);
        bb.put(CRLF);
      }

      // flip the buffer
      bb.flip();

      return bb;
    }
  }

  /**
   *
   */
  public static class MapPipedInsert<T> extends CollectionPipedInsert<T> {

    private static final String COMMAND = "mop insert";
    private final Map<String, T> map;

    public MapPipedInsert(String key, Map<String, T> map,
                          CollectionAttributes attr, Transcoder<T> tc) {
      super(key, attr, tc, map.size());
      if (attr != null) { /* item creation option */
        CollectionCreate.checkOverflowAction(CollectionType.map, attr.getOverflowAction());
      }
      this.map = map;
    }

    public ByteBuffer getAsciiCommand() {
      int capacity = 0;

      // encode values
      List<byte[]> encodedList = new ArrayList<>(map.size());
      CachedData cd = null;
      for (T each : map.values()) {
        cd = tc.encode(each);
        encodedList.add(cd.getData());
      }

      // estimate the buffer capacity
      int i = 0;
      for (String eachMkey : map.keySet()) {
        capacity += KeyUtil.getKeyBytes(key).length;
        capacity += KeyUtil.getKeyBytes(eachMkey).length;
        capacity += encodedList.get(i++).length;
        capacity += 128;
      }

      // allocate the buffer
      ByteBuffer bb = ByteBuffer.allocate(capacity);

      // create ascii operation string
      int mkeySize = map.keySet().size();
      List<String> keyList = new ArrayList<>(map.keySet());
      String createOption = attribute != null ?
          CollectionCreate.makeCreateClause(attribute, cd.getFlags()) : "";
      for (i = this.nextOpIndex; i < mkeySize; i++) {
        String mkey = keyList.get(i);
        byte[] value = encodedList.get(i);
        setArguments(bb, COMMAND, key, mkey, value.length,
                     createOption, (i < mkeySize - 1) ? PIPE : "");
        bb.put(value);
        bb.put(CRLF);
      }

      // flip the buffer
      bb.flip();

      return bb;
    }
  }
}
