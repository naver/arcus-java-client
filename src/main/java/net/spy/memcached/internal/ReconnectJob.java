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
package net.spy.memcached.internal;

import net.spy.memcached.ArcusClient;
import net.spy.memcached.ConnectionFactoryBuilder;

public class ReconnectJob {

  String hostPort;
  String serviceCode;
  ConnectionFactoryBuilder cfb;
  ArcusClient[] client;

  public ReconnectJob(String hostPort, String serviceCode,
                      ConnectionFactoryBuilder cfb, ArcusClient[] client) {
    super();
    this.hostPort = hostPort;
    this.serviceCode = serviceCode;
    this.cfb = cfb;
    this.client = client;
  }

  public String getHostPort() {
    return hostPort;
  }

  public String getServiceCode() {
    return serviceCode;
  }

  public ConnectionFactoryBuilder getCfb() {
    return cfb;
  }

  public ArcusClient[] getClient() {
    return client;
  }

}
