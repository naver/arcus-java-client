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

package net.spy.memcached.auth;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public enum ScramMechanism {
  SCRAM_SHA_256("SHA-256", "HmacSHA256");

  private static final Map<String, ScramMechanism> MECHANISMS_MAP;

  private final String mechanismName;
  private final String hashAlgorithm;
  private final String macAlgorithm;

  static {
    Map<String, ScramMechanism> map = new HashMap<>();
    for (ScramMechanism mech : values()) {
      map.put(mech.mechanismName, mech);
    }
    MECHANISMS_MAP = Collections.unmodifiableMap(map);
  }

  private ScramMechanism(String ha, String ma) {
    mechanismName = "SCRAM-" + ha;
    hashAlgorithm = ha;
    macAlgorithm = ma;
  }

  public final String mechanismName() {
    return mechanismName;
  }

  public String hashAlgorithm() {
    return hashAlgorithm;
  }

  public String macAlgorithm() {
    return macAlgorithm;
  }

  public static ScramMechanism forMechanismName(String mechanismName) {
    return MECHANISMS_MAP.get(mechanismName);
  }

  public static Collection<String> mechanismNames() {
    return MECHANISMS_MAP.keySet();
  }
}
