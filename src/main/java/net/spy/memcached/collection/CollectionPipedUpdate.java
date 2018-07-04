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
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import net.spy.memcached.CachedData;
import net.spy.memcached.KeyUtil;
import net.spy.memcached.collection.ElementFlagFilter.BitWiseOperands;
import net.spy.memcached.transcoders.Transcoder;

public abstract class CollectionPipedUpdate<T> extends CollectionObject {

  public static final String PIPE = "pipe";
  public static final int MAX_PIPED_ITEM_COUNT = 500;

  protected String key;
  protected Transcoder<T> tc;
  protected int itemCount;
  protected int nextOpIndex = 0;

  /**
   * set next index of operation
   * that will be processed after when operation moved by switchover
   */
  public void setNextOpIndex(int i) {
    this.nextOpIndex = i;
  }

  public abstract ByteBuffer getAsciiCommand();

  public abstract ByteBuffer getBinaryCommand();

  public static class BTreePipedUpdate<T> extends CollectionPipedUpdate<T> {

    private static final String COMMAND = "bop update";
    private List<Element<T>> elements;

    public BTreePipedUpdate(String key, List<Element<T>> elements,
                            Transcoder<T> tc) {
      this.key = key;
      this.elements = elements;
      this.tc = tc;
      this.itemCount = elements.size();
    }

    public ByteBuffer getAsciiCommand() {
      int capacity = 0;

      // decode parameters
      List<byte[]> decodedList = new ArrayList<byte[]>(elements.size());
      CachedData cd = null;
      for (Element<T> each : elements) {
        if (each.getValue() != null) {
          cd = tc.encode(each.getValue());
          decodedList.add(cd.getData());
        } else {
          decodedList.add(null);
        }
      }

      // estimate the buffer capacity
      int i = 0;
      ElementFlagUpdate eflagUpdate;
      byte[] value;
      StringBuilder b;

      for (Element<T> each : elements) {
        eflagUpdate = each.getElementFlagUpdate();
        if (eflagUpdate != null) {
          // eflag
          capacity += KeyUtil.getKeyBytes(eflagUpdate.getElementFlagByHex()).length;
          // fwhere bitwop
          if (eflagUpdate.getElementFlagOffset() > -1) {
            capacity += 6;
          }
        }

        capacity += KeyUtil.getKeyBytes(key).length;
        capacity += KeyUtil.getKeyBytes((each.isByteArraysBkey() ? each
                .getBkeyByHex() : String.valueOf(each.getLongBkey()))).length;
        if (decodedList.get(i) != null) {
          capacity += decodedList.get(i++).length;
        }
        capacity += 64;
      }

      // allocate the buffer
      ByteBuffer bb = ByteBuffer.allocate(capacity);

      int eSize = elements.size();
      for (i = this.nextOpIndex; i < eSize; i++) {
        Element<T> element = elements.get(i);
        value = decodedList.get(i);
        eflagUpdate = element.getElementFlagUpdate();
        b = new StringBuilder();

        // has element eflag update
        if (eflagUpdate != null) {
          // use fwhere bitop
          if (eflagUpdate.getElementFlagOffset() > -1 && eflagUpdate.getBitOp() != null) {
            b.append(eflagUpdate.getElementFlagOffset()).append(" ");
            b.append(eflagUpdate.getBitOp()).append(" ");
          }
          b.append(eflagUpdate.getElementFlagByHex());
        }

        setArguments(bb, COMMAND, key,
                (element.isByteArraysBkey() ? element.getBkeyByHex()
                        : String.valueOf(element.getLongBkey())),
                b.toString(), (value == null ? -1 : value.length),
                (i < eSize - 1) ? PIPE : "");
        if (value != null) {
          if (value.length > 0) {
            bb.put(value);
          }
          bb.put(CRLF);
        }
      }

      // flip the buffer
      bb.flip();

      return bb;
    }

    public ByteBuffer getBinaryCommand() {
      throw new RuntimeException("not supported in binary protocol yet.");
    }
  }


  public static class MapPipedUpdate<T> extends CollectionPipedUpdate<T> {

    private static final String COMMAND = "mop update";
    private Map<String, T> elements;

    public MapPipedUpdate(String key, Map<String, T> elements,
                          Transcoder<T> tc) {
      this.key = key;
      this.elements = elements;
      this.tc = tc;
      this.itemCount = elements.size();
    }

    public ByteBuffer getAsciiCommand() {
      int capacity = 0;

      // encode parameters
      List<byte[]> encodedList = new ArrayList<byte[]>(elements.size());
      CachedData cd = null;
      for (T each : elements.values()) {
        cd = tc.encode(each);
        encodedList.add(cd.getData());
      }

      // estimate the buffer capacity
      int i = 0;
      byte[] value;
      StringBuilder b;

      for (String eachMkey : elements.keySet()) {
        capacity += KeyUtil.getKeyBytes(key).length;
        capacity += KeyUtil.getKeyBytes(eachMkey).length;
        capacity += encodedList.get(i++).length;
        capacity += 64;
      }

      // allocate the buffer
      ByteBuffer bb = ByteBuffer.allocate(capacity);

      // create ascii operation string
      int mkeySize = elements.keySet().size();
      List<String> keyList = new ArrayList<String>(elements.keySet());
      for (i = this.nextOpIndex; i < mkeySize; i++) {
        String mkey = keyList.get(i);
        value = encodedList.get(i);
        b = new StringBuilder();

        setArguments(bb, COMMAND, key, mkey,
                b.toString(), (value == null ? -1 : value.length),
                (i < mkeySize - 1) ? PIPE : "");
        if (value != null) {
          if (value.length > 0) {
            bb.put(value);
          }
          bb.put(CRLF);
        }
      }

      // flip the buffer
      bb.flip();

      return bb;
    }

    public ByteBuffer getBinaryCommand() {
      throw new RuntimeException("not supported in binary protocol yet.");
    }
  }

  public String getKey() {
    return key;
  }

  public void setKey(String key) {
    this.key = key;
  }

  public int getItemCount() {
    return this.itemCount;
  }
}
