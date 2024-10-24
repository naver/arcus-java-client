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
package net.spy.memcached;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import javax.management.Attribute;
import javax.management.AttributeList;
import javax.management.AttributeNotFoundException;
import javax.management.DynamicMBean;
import javax.management.MBeanAttributeInfo;
import javax.management.MBeanInfo;

import net.spy.memcached.compat.SpyObject;
import net.spy.memcached.protocol.TCPMemcachedNodeImpl;

/**
 * Arcus java client statistics mbean
 */
public class StatisticsHandler extends SpyObject implements DynamicMBean {

  private static final String ADDED_Q = "addedQ";
  private static final String INPUT_Q = "inputQ";
  private static final String WRITE_Q = "writeQ";
  private static final String READ_Q = "readQ";
  private static final String RECONN_CNT = "reconnectCount";
  private static final String CONT_TIMEOUT = "continuousTimeout";
  private static final String VERSION = "version";

  private static final String DELIMETER = "-";

  private final MemcachedClient client;
  private volatile Map<String, MemcachedNode> nodes = new HashMap<>();

  public StatisticsHandler(final ArcusClient client) {
    this.client = client;
  }

  @Override
  public Object getAttribute(String attribute) throws AttributeNotFoundException {
    if (attribute.contains(ADDED_Q)) {
      return client.getAddedQueueSize();
    }

    TCPMemcachedNodeImpl node = (TCPMemcachedNodeImpl) getNode(attribute);

    if (node == null) {
      return null;
    }

    if (attribute.contains(RECONN_CNT)) {
      return node.getReconnectCount();
    }

    if (attribute.contains(CONT_TIMEOUT)) {
      return node.getContinuousTimeout();
    }

    if (attribute.contains(INPUT_Q)) {
      return node.getInputQueueSize();
    }

    if (attribute.contains(READ_Q)) {
      return node.getReadQueueSize();
    }

    if (attribute.contains(WRITE_Q)) {
      return node.getWriteQueueSize();
    }

    if (attribute.contains(VERSION)) {
      return node.getVersion();
    }

    throw new AttributeNotFoundException("Atrribute '" + attribute
            + "' is not defined.");
  }

  private MemcachedNode getNode(String attribute) {
    if (attribute.contains(DELIMETER)) {
      MemcachedNode memcachedNode = nodes.get(attribute
          .split(DELIMETER, 2)[1]);
      if (memcachedNode instanceof TCPMemcachedNodeImpl) {
        return memcachedNode;
      }
    }

    return null;
  }

  @Override
  public AttributeList getAttributes(String[] attributes) {
    AttributeList list = new AttributeList();

    for (String attribute : attributes) {
      try {
        list.add(new Attribute(attribute, getAttribute(attribute)));
      } catch (Exception e) {
        // Failed to get attributes.
      }
    }

    return list;
  }

  @Override
  public MBeanInfo getMBeanInfo() {
    List<MBeanAttributeInfo> attributes = new ArrayList<>();

    Map<String, MemcachedNode> newNodes = new HashMap<>();

    for (MemcachedNode node : client.getAllNodes()) {
      newNodes.put(node.getSocketAddress().toString(), node);
    }

    nodes = newNodes;

    // global input queue
    attributes.add(new MBeanAttributeInfo(ADDED_Q, "long",
            "added queue size", true, false, false));

    // statistics information on each connection
    for (Entry<String, MemcachedNode> entry : nodes.entrySet()) {
      // reconnect count
      attributes.add(new MBeanAttributeInfo(RECONN_CNT + DELIMETER
              + entry.getValue().getSocketAddress().toString(), "int",
              "reconnect count", true, false, false));

      // continuous timeout count
      attributes.add(new MBeanAttributeInfo(CONT_TIMEOUT + DELIMETER
              + entry.getValue().getSocketAddress().toString(), "int",
              "continuous timeout count", true, false, false));

      // read queue
      attributes.add(new MBeanAttributeInfo(INPUT_Q + DELIMETER
              + entry.getValue().getSocketAddress().toString(), "int",
              "input queue count", true, false, false));

      // read queue
      attributes.add(new MBeanAttributeInfo(READ_Q + DELIMETER
              + entry.getValue().getSocketAddress().toString(), "int",
              "read queue count", true, false, false));

      // write queue
      attributes.add(new MBeanAttributeInfo(WRITE_Q + DELIMETER
              + entry.getValue().getSocketAddress().toString(), "int",
              "write queue count", true, false, false));

      // version
      attributes.add(new MBeanAttributeInfo(VERSION + DELIMETER
              + entry.getValue().getSocketAddress().toString(), "String",
              "version", true, false, false));
    }

    getLogger().debug("retrieve client statistics mbean informations.");

    return new MBeanInfo(this.getClass().getName(),
            "Arcus client statistics MBean",
            attributes.toArray(new MBeanAttributeInfo[0]), null, null, null);
  }

  @Override
  public Object invoke(String actionName, Object[] params, String[] signature) {
    return null;
  }

  @Override
  public void setAttribute(Attribute attribute) {
  }

  @Override
  public AttributeList setAttributes(AttributeList attributes) {
    return null;
  }

}
