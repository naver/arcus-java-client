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
/* ENABLE_MIGRATION if */
package net.spy.memcached;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public abstract class RedirectHandler {

  private Long spoint;
  private Long epoint;

  public abstract void addRedirectKey(String response, String key);

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

  public static class RedirectHandlerSingleKey extends RedirectHandler {

    private String key;
    private String owner;

    @Override
    public void addRedirectKey(String response, String key) {
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

    private List<String> keysWithoutOwner;
    private Map<String, List<String>> keysByOwner;

    @Override
    public void addRedirectKey(String response, String key) {
      String owner = parseRedirectResponse(response);
      if (owner == null) {
        if (keysWithoutOwner == null) {
          keysWithoutOwner = new ArrayList<String>();
        }
        keysWithoutOwner.add(key);
      } else {
        if (keysByOwner == null) {
          keysByOwner = new HashMap<String, List<String>>();
        }
        List<String> keys = keysByOwner.get(owner);
        if (keys == null) {
          keys = new ArrayList<String>();
          keysByOwner.put(owner, keys);
        }
        keys.add(key);
      }
    }

    private Map<MemcachedNode, List<String>> groupRedirectKeysWithOwner(
            MemcachedConnection conn) {
      Map<MemcachedNode, List<String>> keysByNode =
          new HashMap<MemcachedNode, List<String>>();
      for (Map.Entry<String, List<String>> entry : keysByOwner.entrySet()) {
        MemcachedNode node = conn.findNodeByOwner(entry.getKey());
        if (node == null) {
          return null;
        }
        keysByNode.put(node, entry.getValue());
      }
      return keysByNode;
    }

    private Map<MemcachedNode, List<String>> groupRedirectKeysWithoutOwner(
            MemcachedConnection conn) {
      Map<MemcachedNode, List<String>> keysByNode =
          new HashMap<MemcachedNode, List<String>>();
      for (String key : keysWithoutOwner) {
        MemcachedNode node = conn.findNodeByKey(key);
        if (node == null) {
          return null;
        }
        List<String> keys = keysByNode.get(node);
        if (keys == null) {
          keys = new ArrayList<String>();
          keysByNode.put(node, keys);
        }
        keys.add(key);
      }
      return keysByNode;
    }

    public Map<MemcachedNode, List<String>> groupRedirectKeys(
            MemcachedConnection conn) {
      return keysByOwner == null ?
              groupRedirectKeysWithoutOwner(conn) : groupRedirectKeysWithOwner(conn);
    }
  }
}
/* ENABLE_MIGRATION end */
