/*
 * arcus-java-client : Arcus Java client
 * Copyright 2010-2014 NAVER Corp.
 * Copyright 2014-present JaM2in Co., Ltd.
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
package net.spy.memcached.v2.pipe;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import net.spy.memcached.OperationFactory;
import net.spy.memcached.collection.BTreeDelete;
import net.spy.memcached.collection.BTreeInsert;
import net.spy.memcached.collection.BTreeMutate;
import net.spy.memcached.collection.BTreeUpdate;
import net.spy.memcached.collection.BTreeUpsert;
import net.spy.memcached.collection.ListDelete;
import net.spy.memcached.collection.MapDelete;
import net.spy.memcached.collection.MapInsert;
import net.spy.memcached.collection.MapUpdate;
import net.spy.memcached.collection.SetDelete;
import net.spy.memcached.collection.SetExist;
import net.spy.memcached.ops.KeyedOperation;
import net.spy.memcached.ops.Mutator;
import net.spy.memcached.collection.CollectionAttributes;
import net.spy.memcached.collection.ListInsert;
import net.spy.memcached.collection.RequestMode;
import net.spy.memcached.collection.SetInsert;
import net.spy.memcached.transcoders.Transcoder;
import net.spy.memcached.v2.vo.BKey;
import net.spy.memcached.v2.vo.BTreeElement;

/**
 * Data class for pipelined commands.
 * If multiple keys are mapped to the same node, commands for those keys are sent in one batch.
 * The atomicity for commands is not guaranteed but the order of commands
 * for each key is guaranteed.
 * Max number of commands is limited as 500.
 *
 * @param <V>
 */
public class Pipeline<V> {
  private final Set<String> keys;
  private final List<KeyedOperation> ops;
  private final OperationFactory opFact;
  private final Transcoder<V> tc;

  public Pipeline(OperationFactory opFact, Transcoder<V> tc) {
    this.opFact = opFact;
    this.tc = tc;
    this.keys = new HashSet<>();
    this.ops = new ArrayList<>();
  }

  public Pipeline<V> lopInsert(String key, int index, V value) {
    return lopInsert(key, index, value, null);
  }

  public Pipeline<V> lopInsert(String key, int index, V value, CollectionAttributes attributes) {
    checkNumOfCommands();

    ListInsert<V> listInsert = new ListInsert<>(value, RequestMode.PIPE, attributes);
    keys.add(key);
    ops.add(opFact.collectionInsert(key, String.valueOf(index),
        listInsert, tc.encode(value).getData(), null));
    return this;
  }

  public Pipeline<V> lopDelete(String key, int index, boolean dropIfEmpty) {
    checkNumOfCommands();

    ListDelete listDelete = new ListDelete(index, dropIfEmpty, false);
    keys.add(key);
    ops.add(opFact.collectionDelete(key, listDelete, null));
    return this;
  }

  public Pipeline<V> lopDelete(String key, int from, int to, boolean dropIfEmpty) {
    checkNumOfCommands();

    ListDelete listDelete = new ListDelete(from, to, dropIfEmpty, false);
    keys.add(key);
    ops.add(opFact.collectionDelete(key, listDelete, null));
    return this;
  }

  public Pipeline<V> sopInsert(String key, V value) {
    return sopInsert(key, value, null);
  }

  public Pipeline<V> sopInsert(String key, V value, CollectionAttributes attributes) {
    checkNumOfCommands();

    SetInsert<V> setInsert = new SetInsert<>(value, RequestMode.PIPE, attributes);
    keys.add(key);
    ops.add(opFact.collectionInsert(key, "",
        setInsert, tc.encode(value).getData(), null));
    return this;
  }

  public Pipeline<V> sopExist(String key, V value) {
    checkNumOfCommands();

    SetExist<V> setExist = new SetExist<>(value, tc);
    keys.add(key);
    ops.add(opFact.collectionExist(key, "", setExist, null));
    return this;
  }

  public Pipeline<V> sopDelete(String key, V value) {
    checkNumOfCommands();

    SetDelete<V> setDelete = new SetDelete<>(value, false, false, tc);
    keys.add(key);
    ops.add(opFact.collectionDelete(key, setDelete, null));
    return this;
  }

  public Pipeline<V> mopInsert(String key, String mkey, V value) {
    return this.mopInsert(key, mkey, value, null);
  }

  public Pipeline<V> mopInsert(String key, String mkey, V value, CollectionAttributes attributes) {
    checkNumOfCommands();

    MapInsert<V> mapInsert = new MapInsert<>(value, RequestMode.PIPE, attributes);
    keys.add(key);
    ops.add(opFact.collectionInsert(key, mkey,
        mapInsert, tc.encode(value).getData(), null));
    return this;
  }

  public Pipeline<V> mopDelete(String key, String mkey) {
    checkNumOfCommands();

    MapDelete mapDelete = new MapDelete(Collections.singletonList(mkey), false, false);
    keys.add(key);
    ops.add(opFact.collectionDelete(key, mapDelete, null));
    return this;
  }

  public Pipeline<V> mopUpdate(String key, String mkey, V value) {
    checkNumOfCommands();

    MapUpdate<V> mapUpdate = new MapUpdate<>(value, false);
    keys.add(key);
    ops.add(opFact.collectionUpdate(key, mkey, mapUpdate, tc.encode(value).getData(), null));
    return this;
  }

  public Pipeline<V> bopInsert(String key, BTreeElement<V> element) {
    return this.bopInsert(key, element, null);
  }

  public Pipeline<V> bopInsert(String key, BTreeElement<V> element,
                               CollectionAttributes attributes) {
    checkNumOfCommands();

    BTreeInsert<V> mapInsert = new BTreeInsert<>(
        element.getValue(), element.getEFlag(), RequestMode.PIPE, attributes);
    keys.add(key);
    ops.add(opFact.collectionInsert(key, element.getBkey().toString(),
        mapInsert, tc.encode(element.getValue()).getData(), null));
    return this;
  }

  public Pipeline<V> bopUpsert(String key, BTreeElement<V> element) {
    return this.bopUpsert(key, element, null);
  }

  public Pipeline<V> bopUpsert(String key, BTreeElement<V> element,
                               CollectionAttributes attributes) {
    checkNumOfCommands();

    BTreeUpsert<V> mapInsert = new BTreeUpsert<>(
        element.getValue(), element.getEFlag(), RequestMode.PIPE, attributes);
    keys.add(key);
    ops.add(opFact.collectionInsert(key, element.getBkey().toString(),
        mapInsert, tc.encode(element.getValue()).getData(), null));
    return this;
  }

  public Pipeline<V> bopDelete(String key, BKey bkey) {
    checkNumOfCommands();

    BTreeDelete bTreeDelete;
    if (bkey.getType() == BKey.BKeyType.LONG) {
      bTreeDelete = new BTreeDelete((Long) bkey.getData(), null, false, false);
    } else {
      bTreeDelete = new BTreeDelete((byte[]) bkey.getData(), null, false, false);
    }
    keys.add(key);
    ops.add(opFact.collectionDelete(key, bTreeDelete, null));
    return this;
  }

  public Pipeline<V> bopDelete(String key, BKey from, BKey to) {
    checkNumOfCommands();

    if (from.getType() != to.getType()) {
      throw new IllegalArgumentException("BKey types must match for range delete");
    }

    BTreeDelete bTreeDelete;
    if (from.getType() == BKey.BKeyType.LONG) {
      bTreeDelete = new BTreeDelete((Long) from.getData(), (Long) to.getData(),
          -1, null, false, false);
    } else {
      bTreeDelete = new BTreeDelete((byte[]) from.getData(), (byte[]) to.getData(),
          -1, null, false, false);
    }
    keys.add(key);
    ops.add(opFact.collectionDelete(key, bTreeDelete, null));
    return this;
  }

  public Pipeline<V> bopUpdate(String key, BKey bkey, V value) {
    checkNumOfCommands();

    BTreeUpdate<V> bTreeUpdate = new BTreeUpdate<>(value, null, false);
    keys.add(key);
    ops.add(opFact.collectionUpdate(key, bkey.toString(), bTreeUpdate,
        tc.encode(value).getData(), null));
    return this;
  }

  public Pipeline<V> bopIncr(String key, BKey bkey, int delta) {
    checkNumOfCommands();

    BTreeMutate bTreeMutate = new BTreeMutate(Mutator.incr, delta);
    keys.add(key);
    ops.add(opFact.collectionMutate(key, bkey.toString(), bTreeMutate, null));
    return this;
  }

  public Pipeline<V> bopDecr(String key, BKey bkey, int delta) {
    checkNumOfCommands();

    BTreeMutate bTreeMutate = new BTreeMutate(Mutator.decr, delta);
    keys.add(key);
    ops.add(opFact.collectionMutate(key, bkey.toString(), bTreeMutate, null));
    return this;
  }

  public Set<String> getKeys() {
    return Collections.unmodifiableSet(keys);
  }

  public List<KeyedOperation> getOps() {
    return Collections.unmodifiableList(ops);
  }

  private void checkNumOfCommands() {
    if (ops.size() >= 500) {
      throw new IllegalStateException("The number of commands in a pipeline cannot exceed 500");
    }
  }
}
