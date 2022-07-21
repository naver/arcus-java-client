/*
 * arcus-java-client : Arcus Java client
 * Copyright 2022 JaM2in Co., Ltd.
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
package net.spy.memcached;

import net.spy.memcached.ops.OperationStatus;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public abstract class RedirectHandler {

  private Long spoint;
  private Long epoint;
  private OperationStatus operationStatus;

  public abstract void addRedirectKey(String response, String key, Integer keyIndex);

  protected String parseRedirectResponse(String response) {
    /* response format : NOT_MY_KEY <spoint> <epoint> <owner_name> */
    String[] tokens = response.split(" ");
    assert tokens.length >= 3;
    spoint = Long.valueOf(tokens[1]);
    epoint = Long.valueOf(tokens[2]);
    return tokens.length == 3 ? null : tokens[3]; /* no owner in pipe response */
  }

  public final Long getMigrationBasePoint() {
    return spoint;
  }

  public final Long getMigrationEndPoint() {
    return epoint;
  }

  public final void setOperationStatus(OperationStatus operationStatus) {
    this.operationStatus = operationStatus;
  }

  public final OperationStatus getOperationStatus() {
    return operationStatus;
  }

  public static class RedirectHandlerSingleKey extends RedirectHandler {

    private String key;
    private String owner;

    @Override
    public void addRedirectKey(String response, String key, Integer keyIndex) {
      this.key = key;
      this.owner = parseRedirectResponse(response);
    }

    public String getOwner() {
      return owner;
    }

    public String getKey() {
      return key;
    }
  }

  public static class RedirectHandlerMultiKey extends RedirectHandler {

    private List<String> keys;
    private List<Integer> keyIndexes;
    private Map<String, ArrayList<Integer>> keyIndexesByOwner;

    @Override
    public void addRedirectKey(String response, String key, Integer keyIndex) {
      String owner = parseRedirectResponse(response);
      if (owner == null) {
        if (keys == null) {
          keys = new ArrayList<String>();
          keyIndexes = new ArrayList<Integer>();
        }
        keys.add(key);
        keyIndexes.add(keyIndex);
      } else {
        if (keyIndexesByOwner == null) {
          keyIndexesByOwner = new HashMap<String, ArrayList<Integer>>();
        }
        ArrayList<Integer> keyIndexes = keyIndexesByOwner.get(owner);
        if (keyIndexes == null) {
          keyIndexes = new ArrayList<Integer>();
          keyIndexesByOwner.put(owner, keyIndexes);
        }
        keyIndexes.add(keyIndex);
      }
    }

    public Map<MemcachedNode, ArrayList<Integer>> groupRedirectKeysWithOwner(
            MemcachedConnection conn) {
      Map<MemcachedNode, ArrayList<Integer>> keyIndexesByNode =
          new HashMap<MemcachedNode, ArrayList<Integer>>();
      for (Map.Entry<String, ArrayList<Integer>> entry : keyIndexesByOwner.entrySet()) {
        MemcachedNode node = conn.getOwnerNodeByName(entry.getKey());
        if (node == null) {
          return null;
        }
        keyIndexesByNode.put(node, entry.getValue());
      }
      return keyIndexesByNode;
    }

    public Map<MemcachedNode, ArrayList<Integer>> groupRedirectKeysWithoutOwner(
            MemcachedConnection conn) {
      Map<MemcachedNode, ArrayList<Integer>> keyIndexesByNode =
          new HashMap<MemcachedNode, ArrayList<Integer>>();
      for (int i = 0; i < keys.size(); i++) {
        MemcachedNode node = conn.findNodeByKey(keys.get(i));
        if (node == null) {
          return null;
        }
        ArrayList<Integer> keyIndexes = keyIndexesByNode.get(node);
        if (keyIndexes == null) {
          keyIndexes = new ArrayList<Integer>();
          keyIndexesByNode.put(node, keyIndexes);
        }
        keyIndexes.add(this.keyIndexes.get(i));
      }
      return keyIndexesByNode;
    }

    public Map<MemcachedNode, ArrayList<Integer>> groupRedirectKeys(
            MemcachedConnection conn) {
      return keyIndexesByOwner == null ?
              groupRedirectKeysWithoutOwner(conn) : groupRedirectKeysWithOwner(conn);
    }
  }
}
